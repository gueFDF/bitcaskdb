package bitcaskdb

import "errors"

var (
	ErrKeyNotFound  = errors.New("key not found in database")
	ErrInvaliDAFile = errors.New("invalid dbfile")
)


