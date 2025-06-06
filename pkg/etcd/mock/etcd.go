// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/etcd/etcd.go

// Package mock_etcd is a generated GoMock package.
package mock_etcd

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	common "github.com/pingcap/ticdc/pkg/common"
	config "github.com/pingcap/ticdc/pkg/config"
	etcd "github.com/pingcap/ticdc/pkg/etcd"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// MockOwnerCaptureInfoClient is a mock of OwnerCaptureInfoClient interface.
type MockOwnerCaptureInfoClient struct {
	ctrl     *gomock.Controller
	recorder *MockOwnerCaptureInfoClientMockRecorder
}

// MockOwnerCaptureInfoClientMockRecorder is the mock recorder for MockOwnerCaptureInfoClient.
type MockOwnerCaptureInfoClientMockRecorder struct {
	mock *MockOwnerCaptureInfoClient
}

// NewMockOwnerCaptureInfoClient creates a new mock instance.
func NewMockOwnerCaptureInfoClient(ctrl *gomock.Controller) *MockOwnerCaptureInfoClient {
	mock := &MockOwnerCaptureInfoClient{ctrl: ctrl}
	mock.recorder = &MockOwnerCaptureInfoClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockOwnerCaptureInfoClient) EXPECT() *MockOwnerCaptureInfoClientMockRecorder {
	return m.recorder
}

// GetCaptures mocks base method.
func (m *MockOwnerCaptureInfoClient) GetCaptures(arg0 context.Context) (int64, []*config.CaptureInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCaptures", arg0)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].([]*config.CaptureInfo)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetCaptures indicates an expected call of GetCaptures.
func (mr *MockOwnerCaptureInfoClientMockRecorder) GetCaptures(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCaptures", reflect.TypeOf((*MockOwnerCaptureInfoClient)(nil).GetCaptures), arg0)
}

// GetOwnerID mocks base method.
func (m *MockOwnerCaptureInfoClient) GetOwnerID(arg0 context.Context) (config.CaptureID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwnerID", arg0)
	ret0, _ := ret[0].(config.CaptureID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwnerID indicates an expected call of GetOwnerID.
func (mr *MockOwnerCaptureInfoClientMockRecorder) GetOwnerID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwnerID", reflect.TypeOf((*MockOwnerCaptureInfoClient)(nil).GetOwnerID), arg0)
}

// GetOwnerRevision mocks base method.
func (m *MockOwnerCaptureInfoClient) GetOwnerRevision(arg0 context.Context, arg1 config.CaptureID) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwnerRevision", arg0, arg1)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwnerRevision indicates an expected call of GetOwnerRevision.
func (mr *MockOwnerCaptureInfoClientMockRecorder) GetOwnerRevision(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwnerRevision", reflect.TypeOf((*MockOwnerCaptureInfoClient)(nil).GetOwnerRevision), arg0, arg1)
}

// MockCDCEtcdClient is a mock of CDCEtcdClient interface.
type MockCDCEtcdClient struct {
	ctrl     *gomock.Controller
	recorder *MockCDCEtcdClientMockRecorder
}

// MockCDCEtcdClientMockRecorder is the mock recorder for MockCDCEtcdClient.
type MockCDCEtcdClientMockRecorder struct {
	mock *MockCDCEtcdClient
}

// NewMockCDCEtcdClient creates a new mock instance.
func NewMockCDCEtcdClient(ctrl *gomock.Controller) *MockCDCEtcdClient {
	mock := &MockCDCEtcdClient{ctrl: ctrl}
	mock.recorder = &MockCDCEtcdClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCDCEtcdClient) EXPECT() *MockCDCEtcdClientMockRecorder {
	return m.recorder
}

// CheckMultipleCDCClusterExist mocks base method.
func (m *MockCDCEtcdClient) CheckMultipleCDCClusterExist(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckMultipleCDCClusterExist", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckMultipleCDCClusterExist indicates an expected call of CheckMultipleCDCClusterExist.
func (mr *MockCDCEtcdClientMockRecorder) CheckMultipleCDCClusterExist(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckMultipleCDCClusterExist", reflect.TypeOf((*MockCDCEtcdClient)(nil).CheckMultipleCDCClusterExist), ctx)
}

// DeleteCaptureInfo mocks base method.
func (m *MockCDCEtcdClient) DeleteCaptureInfo(arg0 context.Context, arg1 config.CaptureID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteCaptureInfo", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteCaptureInfo indicates an expected call of DeleteCaptureInfo.
func (mr *MockCDCEtcdClientMockRecorder) DeleteCaptureInfo(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteCaptureInfo", reflect.TypeOf((*MockCDCEtcdClient)(nil).DeleteCaptureInfo), arg0, arg1)
}

// GetAllCDCInfo mocks base method.
func (m *MockCDCEtcdClient) GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllCDCInfo", ctx)
	ret0, _ := ret[0].([]*mvccpb.KeyValue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllCDCInfo indicates an expected call of GetAllCDCInfo.
func (mr *MockCDCEtcdClientMockRecorder) GetAllCDCInfo(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllCDCInfo", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetAllCDCInfo), ctx)
}

// GetCaptures mocks base method.
func (m *MockCDCEtcdClient) GetCaptures(arg0 context.Context) (int64, []*config.CaptureInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCaptures", arg0)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].([]*config.CaptureInfo)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetCaptures indicates an expected call of GetCaptures.
func (mr *MockCDCEtcdClientMockRecorder) GetCaptures(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCaptures", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetCaptures), arg0)
}

// GetChangeFeedInfo mocks base method.
func (m *MockCDCEtcdClient) GetChangeFeedInfo(ctx context.Context, id common.ChangeFeedDisplayName) (*config.ChangeFeedInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChangeFeedInfo", ctx, id)
	ret0, _ := ret[0].(*config.ChangeFeedInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetChangeFeedInfo indicates an expected call of GetChangeFeedInfo.
func (mr *MockCDCEtcdClientMockRecorder) GetChangeFeedInfo(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChangeFeedInfo", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetChangeFeedInfo), ctx, id)
}

// GetChangeFeedStatus mocks base method.
func (m *MockCDCEtcdClient) GetChangeFeedStatus(ctx context.Context, id common.ChangeFeedID) (*config.ChangeFeedStatus, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChangeFeedStatus", ctx, id)
	ret0, _ := ret[0].(*config.ChangeFeedStatus)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetChangeFeedStatus indicates an expected call of GetChangeFeedStatus.
func (mr *MockCDCEtcdClientMockRecorder) GetChangeFeedStatus(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChangeFeedStatus", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetChangeFeedStatus), ctx, id)
}

// GetClusterID mocks base method.
func (m *MockCDCEtcdClient) GetClusterID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetClusterID indicates an expected call of GetClusterID.
func (mr *MockCDCEtcdClientMockRecorder) GetClusterID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterID", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetClusterID))
}

// GetEnsureGCServiceID mocks base method.
func (m *MockCDCEtcdClient) GetEnsureGCServiceID(tag string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEnsureGCServiceID", tag)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetEnsureGCServiceID indicates an expected call of GetEnsureGCServiceID.
func (mr *MockCDCEtcdClientMockRecorder) GetEnsureGCServiceID(tag interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEnsureGCServiceID", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetEnsureGCServiceID), tag)
}

// GetEtcdClient mocks base method.
func (m *MockCDCEtcdClient) GetEtcdClient() etcd.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEtcdClient")
	ret0, _ := ret[0].(etcd.Client)
	return ret0
}

// GetEtcdClient indicates an expected call of GetEtcdClient.
func (mr *MockCDCEtcdClientMockRecorder) GetEtcdClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEtcdClient", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetEtcdClient))
}

// GetGCServiceID mocks base method.
func (m *MockCDCEtcdClient) GetGCServiceID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetGCServiceID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetGCServiceID indicates an expected call of GetGCServiceID.
func (mr *MockCDCEtcdClientMockRecorder) GetGCServiceID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetGCServiceID", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetGCServiceID))
}

// GetOwnerID mocks base method.
func (m *MockCDCEtcdClient) GetOwnerID(arg0 context.Context) (config.CaptureID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwnerID", arg0)
	ret0, _ := ret[0].(config.CaptureID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwnerID indicates an expected call of GetOwnerID.
func (mr *MockCDCEtcdClientMockRecorder) GetOwnerID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwnerID", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetOwnerID), arg0)
}

// GetOwnerRevision mocks base method.
func (m *MockCDCEtcdClient) GetOwnerRevision(arg0 context.Context, arg1 config.CaptureID) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwnerRevision", arg0, arg1)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwnerRevision indicates an expected call of GetOwnerRevision.
func (mr *MockCDCEtcdClientMockRecorder) GetOwnerRevision(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwnerRevision", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetOwnerRevision), arg0, arg1)
}

// GetUpstreamInfo mocks base method.
func (m *MockCDCEtcdClient) GetUpstreamInfo(ctx context.Context, upstreamID config.UpstreamID, namespace string) (*config.UpstreamInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUpstreamInfo", ctx, upstreamID, namespace)
	ret0, _ := ret[0].(*config.UpstreamInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUpstreamInfo indicates an expected call of GetUpstreamInfo.
func (mr *MockCDCEtcdClientMockRecorder) GetUpstreamInfo(ctx, upstreamID, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUpstreamInfo", reflect.TypeOf((*MockCDCEtcdClient)(nil).GetUpstreamInfo), ctx, upstreamID, namespace)
}

// PutCaptureInfo mocks base method.
func (m *MockCDCEtcdClient) PutCaptureInfo(arg0 context.Context, arg1 *config.CaptureInfo, arg2 clientv3.LeaseID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutCaptureInfo", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// PutCaptureInfo indicates an expected call of PutCaptureInfo.
func (mr *MockCDCEtcdClientMockRecorder) PutCaptureInfo(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutCaptureInfo", reflect.TypeOf((*MockCDCEtcdClient)(nil).PutCaptureInfo), arg0, arg1, arg2)
}

// MockEtcd is a mock of Etcd interface.
type MockEtcd struct {
	ctrl     *gomock.Controller
	recorder *MockEtcdMockRecorder
}

// MockEtcdMockRecorder is the mock recorder for MockEtcd.
type MockEtcdMockRecorder struct {
	mock *MockEtcd
}

// NewMockEtcd creates a new mock instance.
func NewMockEtcd(ctrl *gomock.Controller) *MockEtcd {
	mock := &MockEtcd{ctrl: ctrl}
	mock.recorder = &MockEtcdMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEtcd) EXPECT() *MockEtcdMockRecorder {
	return m.recorder
}
