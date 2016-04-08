// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"container/list"
	"fmt"
	"os"
	"sync"
	"time"
)

// This log writer sends output to a file
type FileLogWriter struct {
	inRec chan *LogRecord
	rec   chan *LogRecord
	rot   chan bool

	// The opened file
	filename string
	file     *os.File

	// The logging format
	format string

	// File header/trailer
	header, trailer string

	// Rotate at linecount
	maxlines          int
	maxlines_curlines int

	// Rotate at size
	maxsize         int
	maxsize_cursize int

	// Rotate daily
	daily          bool
	daily_opendate int

	// Rotate hour
	hour          bool
	hour_opendate int
	// Keep old logfiles (.001, .002, etc)
	rotate  bool
	closeCh chan struct{}
	msgQ    *list.List
	wg      sync.WaitGroup
}

// This is the FileLogWriter's output method
func (w *FileLogWriter) LogWrite(rec *LogRecord) {
	w.inRec <- rec
}

func (w *FileLogWriter) Close() {
	close(w.closeCh)
	w.wg.Wait()

	for moredata := true; moredata; {
		select {
		case rec := <-w.rec:
			w.write(rec)
		default:
			moredata = false
		}
	}

	for e := w.msgQ.Front(); e != nil; e = e.Next() {
		w.write(e.Value.(*LogRecord))
	}

	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
		w.file.Close()
	}
}

func (w *FileLogWriter) write(rec *LogRecord) {
	if (w.maxlines > 0 && w.maxlines_curlines >= w.maxlines) ||
		(w.maxsize > 0 && w.maxsize_cursize >= w.maxsize) {
		if err := w.intRotate(false); err != nil {
			fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
			return
		}
	}

	now := time.Now()

	if w.daily && now.Day() != w.daily_opendate {
		if err := w.intRotate(true); err != nil {
			fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
			return
		}
	}

	if w.hour && now.Hour() != w.hour_opendate {
		if err := w.intRotate(true); err != nil {
			fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
			return
		}
	}

	// Perform the write
	n, err := fmt.Fprint(w.file, FormatLogRecord(w.format, rec))
	if err != nil {
		fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
		return
	}

	// Update the counts
	w.maxlines_curlines++
	w.maxsize_cursize += n
}

// NewFileLogWriter creates a new LogWriter which writes to the given file and
// has rotation enabled if rotate is true.
//
// If rotate is true, any time a new log file is opened, the old one is renamed
// with a .### extension to preserve it.  The various Set* methods can be used
// to configure log rotation based on lines, size, daily and hour.
//
// The standard log-line format is:
//   [%D %T] [%L] (%S) %M
func NewFileLogWriter(fname string, rotate bool) *FileLogWriter {
	w := &FileLogWriter{
		inRec:    make(chan *LogRecord),
		rec:      make(chan *LogRecord, LogBufferLength),
		rot:      make(chan bool),
		filename: fname,
		format:   "[%D %T] [%L] (%S) %M",
		rotate:   rotate,
		closeCh:  make(chan struct{}),
		msgQ:     list.New(),
	}

	// open the file for the first time
	if err := w.intRotate(false); err != nil {
		fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
		return nil
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		var msg *LogRecord
		var outCh chan *LogRecord
		for {
			select {
			case info := <-w.inRec:
				w.msgQ.PushBack(info)
				if msg == nil {
					msg = w.msgQ.Remove(w.msgQ.Front()).(*LogRecord)
					outCh = w.rec
				}
			case outCh <- msg:
				if w.msgQ.Len() > 0 {
					msg = w.msgQ.Remove(w.msgQ.Front()).(*LogRecord)
				} else {
					msg = nil
					outCh = nil
				}
			case <-w.closeCh:
				if msg != nil {
					w.msgQ.PushFront(msg)
				}
				return
			}
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.rot:
				if err := w.intRotate(false); err != nil {
					/****神一样的Bug---未处理***/
					fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
					return
				}
			case rec := <-w.rec:
				w.write(rec)
			case <-w.closeCh:
				return
			}
		}
	}()

	return w
}

// Request that the logs rotate
func (w *FileLogWriter) Rotate() {
	w.rot <- true
}

// If this is called in a threaded context, it MUST be synchronized
// last is true when hourRotate is set and hour change
func (w *FileLogWriter) intRotate(last bool) error {
	// Close any log file that may be open
	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
		w.file.Close()
	}

	now := time.Now()
	var lastTime time.Time
	if last {
		lastTime = now.Add(time.Duration(-time.Second * 3600))
	} else {
		lastTime = now
	}
	// If we are keeping log files, move it to the next available number
	if w.rotate {
		_, err := os.Lstat(w.filename)
		if err == nil { // file exists
			// Find the next available number
			num := 1
			fname := ""
			for ; err == nil && num <= 999; num++ {
				fname = w.filename + fmt.Sprintf("-%d-%02d-%02d-%02d+", lastTime.Year(), lastTime.Month(), lastTime.Day(), lastTime.Hour()) + fmt.Sprintf("%03d", num)
				_, err = os.Lstat(fname)
			}
			// return error if the last file checked still existed
			if err == nil {
				return fmt.Errorf("Rotate: Cannot find free log number to rename %s\n", w.filename)
			}

			// Rename the file to its newfound home
			err = os.Rename(w.filename, fname)
			if err != nil {
				return fmt.Errorf("Rotate: %s\n", err)
			}
		}
	}

	// Open the log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	w.file = fd

	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: now}))

	// Set the daily open date to the current date
	w.daily_opendate = now.Day()

	w.hour_opendate = now.Hour()

	// initialize rotation values
	w.maxlines_curlines = 0
	w.maxsize_cursize = 0

	return nil
}

// Set the logging format (chainable).  Must be called before the first log
// message is written.
func (w *FileLogWriter) SetFormat(format string) *FileLogWriter {
	w.format = format
	return w
}

// Set the logfile header and footer (chainable).  Must be called before the first log
// message is written.  These are formatted similar to the FormatLogRecord (e.g.
// you can use %D and %T in your header/footer for date and time).
func (w *FileLogWriter) SetHeadFoot(head, foot string) *FileLogWriter {
	w.header, w.trailer = head, foot
	if w.maxlines_curlines == 0 {
		fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: time.Now()}))
	}
	return w
}

// Set rotate at linecount (chainable). Must be called before the first log
// message is written.
func (w *FileLogWriter) SetRotateLines(maxlines int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateLines: %v\n", maxlines)
	w.maxlines = maxlines
	return w
}

// Set rotate at size (chainable). Must be called before the first log message
// is written.
func (w *FileLogWriter) SetRotateSize(maxsize int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateSize: %v\n", maxsize)
	w.maxsize = maxsize
	return w
}

// Set rotate daily (chainable). Must be called before the first log message is
// written.
func (w *FileLogWriter) SetRotateDaily(daily bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateDaily: %v\n", daily)
	w.daily = daily
	return w
}

func (w *FileLogWriter) SetRotateHour(hour bool) *FileLogWriter {
	w.hour = hour
	return w
}

// SetRotate changes whether or not the old logs are kept. (chainable) Must be
// called before the first log message is written.  If rotate is false, the
// files are overwritten; otherwise, they are rotated to another file before the
// new log is opened.
func (w *FileLogWriter) SetRotate(rotate bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotate: %v\n", rotate)
	w.rotate = rotate
	return w
}

// NewXMLLogWriter is a utility method for creating a FileLogWriter set up to
// output XML record log messages instead of line-based ones.
func NewXMLLogWriter(fname string, rotate bool) *FileLogWriter {
	return NewFileLogWriter(fname, rotate).SetFormat(
		`	<record level="%L">
		<timestamp>%D %T</timestamp>
		<source>%S</source>
		<message>%M</message>
	</record>`).SetHeadFoot("<log created=\"%D %T\">", "</log>")
}
