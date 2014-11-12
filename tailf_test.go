package tailf_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aybabtme/tailf"
)

func TestCanFollowFile(t *testing.T) { withTempFile(t, canFollowFile) }

func canFollowFile(t *testing.T, filename string, file *os.File) error {

	toWrite := []string{
		"hello,",
		" world!",
	}

	want := strings.Join(toWrite, "")

	follow, err := tailf.Follow(filename, true)
	if err != nil {
		t.Fatalf("Failed creating tailf.follower: '%v'", err)
	}

	go func() {
		for _, str := range toWrite {
			t.Logf("Writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Fatalf("Failed to write to file: '%v'", err)
			}
		}
	}()

	// this should work, without blocking forever
	data := make([]byte, len(want))
	_, err = io.ReadAtLeast(follow, data, len(want))
	if err != nil {
		return err
	}

	// this should block forever
	errc := make(chan error, 1)
	go func() {
		n, err := follow.Read(make([]byte, 1))
		t.Logf("Read %d bytes after closing", n)
		errc <- err
	}()

	if err := follow.Close(); err != nil {
		t.Errorf("Failed to close tailf.follower: %v", err)
	}

	got := string(data)
	if want != got {
		t.Errorf("Wanted '%v', got '%v'", want, got)
	}

	err = <-errc
	if err != io.EOF {
		t.Errorf("Expected EOF after closing the follower, got '%v' instead", err)
	}

	return nil
}

func TestCanFollowFileFromEnd(t *testing.T) { withTempFile(t, canFollowFileFromEnd) }

func canFollowFileFromEnd(t *testing.T, filename string, file *os.File) error {

	_, err := file.WriteString("shouldn't read this part")
	if err != nil {
		return err
	}

	toWrite := []string{
		"hello,",
		" world!",
	}

	want := strings.Join(toWrite, "")

	follow, err := tailf.Follow(filename, false)
	if err != nil {
		t.Fatalf("Failed creating tailf.follower: %v", err)
	}

	go func() {
		for _, str := range toWrite {
			t.Logf("Writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Fatalf("Failed to write to file: '%v'", err)
			}
		}
	}()

	// this should work, without blocking forever
	data := make([]byte, len(want))
	_, err = io.ReadAtLeast(follow, data, len(want))
	if err != nil {
		return err
	}

	// this should block forever
	errc := make(chan error, 1)
	go func() {
		n, err := io.ReadAtLeast(follow, make([]byte, 1), 1)
		t.Logf("Read %d bytes after closing", n)
		errc <- err
	}()

	if err := follow.Close(); err != nil {
		t.Errorf("Failed to close tailf.follower: %v", err)
	}

	got := string(data)
	if want != got {
		t.Errorf("Wanted '%v', got '%v'", want, got)
	}

	err = <-errc
	if err != io.EOF {
		t.Errorf("Expected EOF after closing the follower, got '%v' instead", err)
	}

	return nil
}

func TestFollowTruncation(t *testing.T) { withTempFile(t, canFollowTruncation) }

func canFollowTruncation(t *testing.T, filename string, file *os.File) error {
	follow, err := tailf.Follow(filename, false)
	if err != nil {
		t.Fatalf("Failed creating tailf.follower: %v", err)
	}

	for i := int64(0); i < 10; i++ {
		if i%2 == 0 {
			t.Logf("Truncating the file")
			file, err := os.OpenFile(filename, os.O_TRUNC, os.ModeTemporary)
			if err != nil {
				t.Errorf("Unable to truncate file")
			}
			file.Close()
		}

		expected := strconv.AppendInt(make([]byte, 0), i, 10)
		_, err = file.WriteString(string(expected))
		if err != nil {
			t.Error("Write failed")
		}

		test_buf := make([]byte, 1)
		follow.Read(test_buf)
		t.Logf("ReadByte: Actual(%v) ?= Expected(%v)", test_buf, expected)
		if !bytes.Equal(test_buf, expected) {
			t.Errorf("Missed write after truncation")
		}
	}

	if err := follow.Close(); err != nil {
		t.Errorf("Failed to close tailf.follower: %v", err)
	}

	return nil
}

func withTempFile(t *testing.T, action func(t *testing.T, filename string, file *os.File) error) {
	timeout := time.AfterFunc(time.Second*3, func() { panic("too long") })
	defer timeout.Stop()

	file, err := ioutil.TempFile(os.TempDir(), "tailf_test")
	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	err = action(t, file.Name(), file)
	if err != nil {
		t.Errorf("failure: %v", err)
	}
}
