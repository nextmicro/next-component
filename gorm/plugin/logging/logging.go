package logging

import (
	"context"
	"errors"
	"time"

	"github.com/nextmicro/gokit/timex"
	"github.com/nextmicro/logger"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
	"gorm.io/gorm/utils"
)

type options struct {
	infoFormat     string
	warnFormat     string
	errorFormat    string
	traceFormat    string
	slowFormat     string
	traceErrFormat string
	Level          glogger.LogLevel
	SlowThreshold  time.Duration
}

type Option func(*options)

func WithLevel(level glogger.LogLevel) Option {
	return func(o *options) {
		o.Level = level
	}
}

func WithSlowThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.SlowThreshold = threshold
	}
}

type logging struct {
	opt options
}

func NewLogging(opts ...Option) glogger.Interface {
	cfg := options{
		infoFormat:     "%s\n[info] ",
		warnFormat:     "%s\n[warn] ",
		errorFormat:    "%s\n[error] ",
		traceFormat:    "%s\n[%.3fms] [rows:%v] %s",
		slowFormat:     "%s %s\n[%.3fms] [rows:%v] %s",
		traceErrFormat: "%s %s\n[%.3fms] [rows:%v] %s",
		Level:          glogger.Error,
		SlowThreshold:  100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &logging{opt: cfg}
}

func (log *logging) LogMode(level glogger.LogLevel) glogger.Interface {
	log.opt.Level = level
	return log
}

func (log *logging) Info(ctx context.Context, msg string, data ...interface{}) {
	if log.opt.Level >= glogger.Info {
		logger.WithContext(ctx).Infof(log.opt.infoFormat+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

func (log *logging) Warn(ctx context.Context, msg string, data ...interface{}) {
	if log.opt.Level >= glogger.Warn {
		logger.WithContext(ctx).Warnf(log.opt.warnFormat+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

func (log *logging) Error(ctx context.Context, msg string, data ...interface{}) {
	if log.opt.Level >= glogger.Error {
		logger.WithContext(ctx).Errorf(log.opt.errorFormat+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

// Trace print sql message
func (log *logging) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if log.opt.Level <= glogger.Silent {
		return
	}

	sql, rows := fc()
	elapsed := time.Since(begin)
	fields := map[string]interface{}{
		"kind":      "db",
		"component": "mysql",
		"statement": sql,
		"rows":      rows,
		"start":     begin.Format("2006-01-02T15:04:05.999Z0700"),
		"duration":  timex.Duration(elapsed),
		"caller":    utils.FileWithLineNum(),
	}
	if err != nil {
		fields["error"] = err
	}

	logx := logger.WithContext(ctx).WithFields(fields)

	switch {
	case err != nil && !errors.Is(err, gorm.ErrRecordNotFound):
		logx.Error("mysql client")
	case elapsed >= log.opt.SlowThreshold && log.opt.SlowThreshold != 0:
		logx.Info("mysql client slow")
	case log.opt.Level >= glogger.Error:
		logx.Info("mysql client")
	}
}
