package logger

import (
	"fmt"
	"github.com/HDT3213/godis/src/lib/files"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type Settings struct {
	Path    string `yaml:"path"`
	Name    string `yaml:"name"`
	Ext     string `yaml:"ext"`
	Timeout string `yaml:"time-format"`
}

var (
	F                  *os.File
	DefaultPrefix      = ""
	DefaultCallerDepth = 2
	logger             *log.Logger
	logPrefix          = ""
	levelFlags         = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func Setup(settings *Settings) {
	var err error
	dir := settings.Path
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.Timeout),
		settings.Ext)

	// MustOpen 是dir存在且权限正确并正确打开
	logFile, err := files.MustOpen(fileName, dir)
	if err != nil {
		log.Fatal("logging.Setup err: %s", err)
	}
	// 将log写入文件和标准输出
	mw := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(mw, DefaultPrefix, log.LstdFlags)
}

func setPrefix(level Level) {
	_, file, line, ok := runtime.Caller(DefaultCallerDepth)
	if ok {
		logPrefix = fmt.Sprintf("[%s][%s:%d] ", levelFlags[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("[%s] ", levelFlags[level])
	}

	logger.SetPrefix(logPrefix)
}

func Debug(v ...interface{}) {
	setPrefix(DEBUG)
	logger.Println(v)
}

func Info(v ...interface{}) {
	setPrefix(INFO)
	logger.Println(v)
}

func Warn(v ...interface{}) {
	setPrefix(WARNING)
	logger.Println(v)
}

func Error(v ...interface{}) {
	setPrefix(ERROR)
	logger.Println(v)
}

func Fatal(v ...interface{}) {
	setPrefix(FATAL)
	logger.Fatalln(v)
}
