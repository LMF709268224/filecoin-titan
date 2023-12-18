package scheduler

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/terrors"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/user"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"golang.org/x/xerrors"
)

const (
	userFileGroupMaxCount = 100
	rootGroup             = 0
)

// UserAPIKeysExists checks if the user exists.
func (s *Scheduler) UserAPIKeysExists(ctx context.Context, userID string) error {
	u := s.newUser(userID)
	keys, err := u.GetAPIKeys(ctx)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return fmt.Errorf("user %s api keys not exist", userID)
	}

	return nil
}

// AllocateStorage allocates storage space.
func (s *Scheduler) AllocateStorage(ctx context.Context, userID string) (*types.UserInfo, error) {
	u := s.newUser(userID)

	info, err := u.AllocateStorage(ctx, s.SchedulerCfg.UserFreeStorageSize)
	if err != nil {
		return nil, xerrors.Errorf("AllocateStorage err:%s", err.Error())
	}

	return info, nil
}

// GetUserInfo get user info
func (s *Scheduler) GetUserInfo(ctx context.Context, userID string) (*types.UserInfo, error) {
	return s.loadUserInfo(userID)
}

// GetUserInfos get user infos
func (s *Scheduler) GetUserInfos(ctx context.Context, userIDs []string) (map[string]*types.UserInfo, error) {
	out := make(map[string]*types.UserInfo, 0)
	for _, userID := range userIDs {
		info, err := s.loadUserInfo(userID)
		if err != nil {
			continue
		}

		out[userID] = info
	}

	return out, nil
}

func (s *Scheduler) loadUserInfo(userID string) (*types.UserInfo, error) {
	u := s.newUser(userID)
	return u.GetInfo()
}

// CreateAPIKey creates a key for the client API.
func (s *Scheduler) CreateAPIKey(ctx context.Context, userID, keyName string, perms []types.UserAccessControl) (string, error) {
	u := s.newUser(userID)
	info, err := u.CreateAPIKey(ctx, keyName, perms, s.SchedulerCfg, s.CommonAPI)
	if err != nil {
		return "", err
	}

	return info, nil
}

// GetAPIKeys get all api key for user.
func (s *Scheduler) GetAPIKeys(ctx context.Context, userID string) (map[string]types.UserAPIKeysInfo, error) {
	u := s.newUser(userID)
	info, err := u.GetAPIKeys(ctx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *Scheduler) DeleteAPIKey(ctx context.Context, userID, name string) error {
	u := s.newUser(userID)
	return u.DeleteAPIKey(ctx, name)
}

func (s *Scheduler) UpdateShareStatus(ctx context.Context, userID, assetCID string) error {
	u := s.newUser(userID)
	return u.SetAssetAtShareStatus(ctx, assetCID)
}

func (s *Scheduler) newUser(userID string) *user.User {
	return &user.User{ID: userID, SQLDB: s.AssetManager.SQLDB, Manager: s.AssetManager}
}

// UserAssetDownloadResult download result
func (s *Scheduler) UserAssetDownloadResult(ctx context.Context, userID, cid string, totalTraffic, peakBandwidth int64) error {
	nodeID := handler.GetNodeID(ctx)

	cNode := s.NodeManager.GetNode(nodeID)
	if cNode == nil {
		return xerrors.Errorf("UserAssetDownloadResult node not found: %s", nodeID)
	}

	err := s.db.UpdateUserInfo(userID, totalTraffic, 1)
	if err != nil {
		return err
	}

	return s.db.UpdateUserPeakSize(userID, peakBandwidth)
}

func (s *Scheduler) SetUserVIP(ctx context.Context, userID string, enableVIP bool) error {
	storageSize := s.SchedulerCfg.UserFreeStorageSize
	if enableVIP {
		storageSize = s.SchedulerCfg.UserVipStorageSize
	}
	return s.db.UpdateUserVIPAndStorageSize(userID, enableVIP, storageSize)
}

func (s *Scheduler) GetUserAccessToken(ctx context.Context, userID string) (string, error) {
	_, err := s.GetUserInfo(ctx, userID)
	if err != nil {
		return "", err
	}

	payload := types.JWTPayload{ID: userID, Allow: []auth.Permission{api.RoleUser}, AccessControlList: types.UserAccessControlAll}
	tk, err := s.AuthNew(ctx, &payload)
	if err != nil {
		return "", err
	}
	return tk, nil
}

// GetUserStorageStats get user storage info
func (s *Scheduler) GetUserStorageStats(ctx context.Context, userID string) (*types.StorageStats, error) {
	return s.db.LoadStorageStatsOfUser(userID)
}

// ListUserStorageStats list storage info
func (s *Scheduler) ListUserStorageStats(ctx context.Context, limit, offset int) (*types.ListStorageStatsRsp, error) {
	return s.db.ListStorageStatsOfUsers(limit, offset)
}

// CreateAssetGroup create file group
func (s *Scheduler) CreateAssetGroup(ctx context.Context, parent int, name, userID string) (*types.AssetGroup, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	if parent > rootGroup {
		exist, err := s.db.AssetGroupExists(userID, parent)
		if err != nil {
			return nil, err
		}

		if !exist {
			return nil, fmt.Errorf("CreateAssetGroup failed, group parent [%d] is not exist ", parent)
		}
	}

	count, err := s.db.GetAssetGroupCount(userID)
	if err != nil {
		return nil, err
	}

	if count >= userFileGroupMaxCount {
		return nil, fmt.Errorf("CreateAssetGroup failed, Exceed the limit %d", userFileGroupMaxCount)
	}

	info, err := s.db.CreateAssetGroup(&types.AssetGroup{UserID: userID, Parent: parent, Name: name})
	if err != nil {
		return nil, err
	}

	return info, nil
}

// ListAssetGroup list file group
func (s *Scheduler) ListAssetGroup(ctx context.Context, parent int, userID string, limit int, offset int) (*types.ListAssetGroupRsp, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.db.ListAssetGroupForUser(userID, parent, limit, offset)
}

// ListAssetSummary list file group
func (s *Scheduler) ListAssetSummary(ctx context.Context, gid int, userID string, limit int, offset int) (*types.ListAssetSummaryRsp, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	out := new(types.ListAssetSummaryRsp)

	// list group
	groupRsp, err := s.db.ListAssetGroupForUser(userID, gid, limit, offset)
	if err != nil {
		return nil, xerrors.Errorf("ListAssetGroupForUser err:%s", err.Error())
	}

	for _, group := range groupRsp.AssetGroups {
		i := new(types.UserAssetSummary)
		i.AssetGroup = group
		out.List = append(out.List, i)
	}

	out.Total += groupRsp.Total

	aLimit := limit - groupRsp.Total
	if aLimit < 0 {
		aLimit = 0
	}

	aOffset := offset - groupRsp.Total
	if aOffset < 0 {
		aOffset = 0
	}

	u := s.newUser(userID)
	assetRsp, err := u.ListAssets(ctx, aLimit, aOffset, s.SchedulerCfg.MaxCountOfVisitShareLink, gid)
	if err != nil {
		return nil, xerrors.Errorf("ListAssets err:%s", err.Error())
	}

	for _, asset := range assetRsp.AssetOverviews {
		i := new(types.UserAssetSummary)
		i.AssetOverview = asset
		out.List = append(out.List, i)
	}

	out.Total += assetRsp.Total

	return out, nil
}

// DeleteAssetGroup delete asset group
func (s *Scheduler) DeleteAssetGroup(ctx context.Context, gid int, userID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	gCount, err := s.db.GetUserAssetCountByGroupID(userID, gid)
	if err != nil {
		return err
	}

	if gCount > 0 {
		return fmt.Errorf("There are assets in the group and the group cannot be deleted")
	}

	rsp, err := s.db.ListAssetGroupForUser(userID, gid, 1, 0)
	if err != nil {
		return err
	}

	if rsp.Total > 0 {
		return fmt.Errorf("There are groups in the group and the group cannot be deleted")
	}

	// delete asset
	// u := s.newUser(userID)
	// u.DeleteAsset(ctx, assetCID)

	return s.db.DeleteAssetGroup(userID, gid)
}

// RenameAssetGroup rename group
func (s *Scheduler) RenameAssetGroup(ctx context.Context, groupID int, rename, userID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.db.UpdateAssetGroupName(userID, rename, groupID)
}

// MoveAssetToGroup move a file to group
func (s *Scheduler) MoveAssetToGroup(ctx context.Context, cid string, groupID int, userID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	hash, err := cidutil.CIDToHash(cid)
	if err != nil {
		return err
	}

	return s.db.UpdateAssetGroup(hash, userID, groupID)
}

// MoveAssetGroup move a asset group
func (s *Scheduler) MoveAssetGroup(ctx context.Context, groupID int, userID string, targetGroupID int) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	if targetGroupID > rootGroup {
		exist, err := s.db.AssetGroupExists(userID, targetGroupID)
		if err != nil {
			return err
		}

		if !exist {
			return fmt.Errorf("MoveAssetGroup failed, group parent [%d] is not exist ", targetGroupID)
		}
	}

	return s.db.UpdateAssetGroupParent(userID, groupID, targetGroupID)
}

// GetAPPKeyPermissions get the permission of user app key
func (s *Scheduler) GetAPPKeyPermissions(ctx context.Context, userID string, keyName string) ([]string, error) {
	keyMap, err := s.GetAPIKeys(ctx, userID)
	if err != nil {
		return nil, err
	}

	key, ok := keyMap[keyName]
	if !ok {
		return nil, &api.ErrWeb{Code: terrors.APPKeyNotFound.Int(), Message: fmt.Sprintf("the API key %s already exist", keyName)}
	}

	payload, err := s.AuthVerify(ctx, key.APIKey)
	if err != nil {
		return nil, err
	}

	permissions := make([]string, 0, len(payload.AccessControlList))
	for _, accessControl := range payload.AccessControlList {
		permissions = append(permissions, string(accessControl))
	}
	return permissions, nil
}
