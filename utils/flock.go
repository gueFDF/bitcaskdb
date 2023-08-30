package utils

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

//此文件，封装文件锁

/*
go中的文件锁
syscall.Flock(fd int , how int)函数进行枷锁

how:
syscall.LOCK_EX :获取的是排他锁(加锁后只能自己使用)
syscall.LOCK_SH :获取的是共享锁(加锁后共享锁可以访问，排他不可以访问)
syscall.LOCK_UN :解锁

syscall.LOCK_NB :表示当前获取锁的模式是非阻塞模式，如果需要阻塞模式，不加这个参数即可(默认是阻塞)
*/

type FLock struct {
	fileName string
	lock     *os.File
}

func NewFLock(filename string) (f *FLock, e error) {

	if filename == "" {
		e = errors.New("cannot create flock on empty path")
		return

	}
	filename += ".lock"
	lock, e := os.Create(filename)
	if e != nil {
		return
	}

	f = &FLock{fileName: filename, lock: lock}
	return
}

// 加锁
func (l *FLock) Lock() error {
	if l == nil {
		return errors.New("cannot use lock on a nil flock")
	}
	err := syscall.Flock(int(l.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s (in use by other)", l.fileName, err)
	}
	return nil
}

// 解锁
func (l *FLock) Unlock() error {
	if l != nil {
		return syscall.Flock(int(l.lock.Fd()), syscall.LOCK_UN)
	}
	return nil
}

func (l *FLock) Release() {
	if l != nil && l.lock != nil {
		l.lock.Close()
		os.Remove(l.fileName)
	}

}
