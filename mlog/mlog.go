package mlog

import (
	"fmt"
	"log"
	"os"
)

type Level int

// 枚举日志等级
const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	FATAL
)

// flags
const (
	Ldate         = log.Ldate         //eg:2009/01/23
	Ltime         = log.Ltime         //eg:01:23:23
	Lmicroseconds = log.Lmicroseconds //01:23:23.123123
	Llongfile     = log.Llongfile     //完整的文件名和行号
	Lshortfile    = log.Lshortfile    //最后一个文件名和行号
	LstdFlags     = log.LstdFlags     //标准（默认）
)

// 配置（目前较为简单）
var (
	LogLevel  Level       //日志等级
	outfil    *os.File    //输出文件
	logPrefix string      //日志前缀
	logger    *log.Logger //向终端的输出
)

// 用来设置前缀
var levelFlags = []string{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"}

// 初始化默认配置
func init() {
	LogLevel = DEBUG
	logPrefix = ""
	outfil = os.Stdout //默认向终端输出
	logger = log.New(outfil, "[default]", LstdFlags)
}

// Println ..
func Println(l *log.Logger, v ...interface{}) {
	if l != nil {
		l.Output(3, fmt.Sprintln(v...))
	}
}
func Printf(l *log.Logger, format string, v ...interface{}) {
	if l != nil {
		l.Output(3, fmt.Sprintf(format, v...))
	}
}

// Fatalln is equivalent to l.Println() followed by a call to os.Exit(1).
func Fatalln(l *log.Logger, v ...interface{}) {
	if l != nil {
		l.Output(3, fmt.Sprintln(v...))
		os.Exit(1)
	}
}

func Fatalf(l *log.Logger, format string, v ...interface{}) {
	if l != nil {
		l.Output(3, fmt.Sprintf(format, v...))
		os.Exit(1)
	}
}

// Debug ...
func Debug(format string, v ...interface{}) {
	setPrefix(DEBUG)
	if DEBUG >= LogLevel {
		Printf(logger, format, v...)
	}

}

// Info ...
func Info(format string, v ...interface{}) {
	setPrefix(INFO)
	if INFO >= LogLevel {
		Printf(logger, format, v...)
	}
}

// Warn ...
func Warn(format string, v ...interface{}) {
	setPrefix(WARNING)
	if WARNING >= LogLevel {
		Printf(logger, format, v...)
	}
}

// Error Warn
func Error(format string, v ...interface{}) {
	setPrefix(ERROR)
	if ERROR >= LogLevel {
		Printf(logger, format, v...)
	}
}

// Fatal ...
func Fatal(v ...interface{}) {
	setPrefix(FATAL)
	if FATAL >= LogLevel {
		Fatalln(logger, v...)
	}

}
func setPrefix(level Level) {
	logPrefix = fmt.Sprintf("[%s] ", levelFlags[level])
	logger.SetPrefix(logPrefix)
}

func SetFlags(flag int) {
	logger.SetFlags(flag)
}
