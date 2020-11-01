// Code generated by mockery v2.3.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// APIHealth is an autogenerated mock type for the APIHealth type
type APIHealth struct {
	mock.Mock
}

// Get provides a mock function with given fields:
func (_m *APIHealth) Get() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: health
func (_m *APIHealth) Update(health bool) error {
	ret := _m.Called(health)

	var r0 error
	if rf, ok := ret.Get(0).(func(bool) error); ok {
		r0 = rf(health)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
