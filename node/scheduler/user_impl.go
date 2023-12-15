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

const userFileGroupMaxCount = 100

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
func (s *Scheduler) CreateAPIKey(ctx context.Context, userID, keyName string) (string, error) {
	u := s.newUser(userID)
	keys, err := u.GetAPIKeys(ctx)
	if err != nil {
		return "", err
	}

	if len(keys) >= s.SchedulerCfg.MaxAPIKey {
		return "", &api.ErrWeb{Code: terrors.OutOfMaxAPIKeyLimit.Int(), Message: fmt.Sprintf("api key exceeds maximum limit %d", s.SchedulerCfg.MaxAPIKey)}
	}

	info, err := u.CreateAPIKey(ctx, keyName, s.CommonAPI)
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

	payload := types.JWTPayload{ID: userID, Allow: []auth.Permission{api.RoleUser}}
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
func (s *Scheduler) CreateAssetGroup(ctx context.Context, parent int, name, userID string) ([]*types.AssetGroup, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	count, err := s.db.GetAssetGroupCount(userID)
	if err != nil {
		return nil, err
	}

	if count >= userFileGroupMaxCount {
		return nil, fmt.Errorf("CreateAssetGroup failed, Exceed the limit %d", userFileGroupMaxCount)
	}

	err = s.db.CreateAssetGroup(&types.AssetGroup{UserID: userID, Parent: parent, Name: name})
	if err != nil {
		return nil, err
	}

	return s.db.ListAssetGroupForUser(userID, parent)
}

// ListAssetGroup list file group
func (s *Scheduler) ListAssetGroup(ctx context.Context, parent int, userID string) ([]*types.AssetGroup, error) {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	return s.db.ListAssetGroupForUser(userID, parent)
}

// DeleteAssetGroup delete asset group
func (s *Scheduler) DeleteAssetGroup(ctx context.Context, gid int, userID string) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		userID = uID
	}

	aList, err := s.db.ListAllAssetsForUser(userID, gid)
	if err != nil {
		return err
	}

	if len(aList) > 0 {
		return fmt.Errorf("There are assets in the group and the group cannot be deleted")
	}

	gList, err := s.db.ListAssetGroupForUser(userID, gid)
	if err != nil {
		return err
	}

	if len(gList) > 0 {
		return fmt.Errorf("There are groups in the group and the group cannot be deleted")
	}

	// delete asset
	// u := s.newUser(userID)
	// u.DeleteAsset(ctx, assetCID)

	return s.db.DeleteAssetGroup(userID, gid)
}

// RenameAssetGroup rename group
func (s *Scheduler) RenameAssetGroup(ctx context.Context, info *types.AssetGroup) error {
	uID := handler.GetUserID(ctx)
	if len(uID) > 0 {
		info.UserID = uID
	}

	return s.db.UpdateAssetGroupName(info)
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
