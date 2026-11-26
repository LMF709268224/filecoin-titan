package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
)

type deactivationPermissionScheduler struct {
	api.SchedulerStub
	calls  []string
	nodeID string
	hours  int
	rate   float64
}

func (s *deactivationPermissionScheduler) DeactivateNode(_ context.Context, nodeID string, hours int) error {
	s.calls = append(s.calls, "DeactivateNode")
	s.nodeID, s.hours = nodeID, hours
	return nil
}

func (s *deactivationPermissionScheduler) DeactivateNodeV2(_ context.Context, nodeID string, hours int, rate float64) error {
	s.calls = append(s.calls, "DeactivateNodeV2")
	s.nodeID, s.hours, s.rate = nodeID, hours, rate
	return nil
}

func TestSchedulerDeactivationPermissions(t *testing.T) {
	roles := []struct {
		name    string
		perms   []auth.Permission
		allowed bool
	}{
		{name: "anonymous"},
		{name: "default", perms: []auth.Permission{api.RoleDefault}},
		{name: "edge", perms: []auth.Permission{api.RoleEdge}},
		{name: "user", perms: []auth.Permission{api.RoleUser}},
		{name: "candidate", perms: []auth.Permission{api.RoleCandidate}},
		{name: "web", perms: []auth.Permission{api.RoleWeb}, allowed: true},
		{name: "admin", perms: []auth.Permission{api.RoleAdmin}, allowed: true},
	}
	methods := []struct {
		name   string
		params string
	}{
		{name: "DeactivateNode", params: `["c_permission_test",1]`},
		{name: "DeactivateNodeV2", params: `["c_permission_test",1,0.6]`},
	}

	for _, method := range methods {
		for _, role := range roles {
			t.Run(method.name+"/"+role.name, func(t *testing.T) {
				s := &deactivationPermissionScheduler{}
				rpc := jsonrpc.NewServer()
				rpc.Register("titan", api.PermissionedSchedulerAPI(s))
				verify := func(context.Context, string) (*types.JWTPayload, error) {
					return &types.JWTPayload{ID: "c_permission_test", Allow: role.perms}, nil
				}
				h := handler.New(verify, rpc.ServeHTTP)
				body := `{"jsonrpc":"2.0","id":1,"method":"titan.` + method.name + `","params":` + method.params + `}`
				req := httptest.NewRequest("POST", "/rpc/v0", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				if role.perms != nil {
					req.Header.Set("Authorization", "Bearer test-credential")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				var response struct {
					Error *struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatalf("invalid RPC response: %v: %s", err, w.Body.String())
				}
				if !role.allowed {
					if len(s.calls) != 0 {
						t.Fatalf("unauthorized %s request reached deactivation implementation", role.name)
					}
					if response.Error == nil || !strings.Contains(response.Error.Message, "missing permission to invoke") {
						t.Fatalf("expected permission rejection, got %s", w.Body.String())
					}
					return
				}
				if response.Error != nil || len(s.calls) != 1 || s.calls[0] != method.name {
					t.Fatalf("authorized request did not reach implementation: calls=%v, response=%s", s.calls, w.Body.String())
				}
				if s.nodeID != "c_permission_test" || s.hours != 1 || (method.name == "DeactivateNodeV2" && s.rate != 0.6) {
					t.Fatalf("request parameters changed: nodeID=%q hours=%d rate=%f", s.nodeID, s.hours, s.rate)
				}
			})
		}
	}
}
