// Taken non-verbatim from https://github.com/jimlawless/cfg
package util

import (
	"errors"
	"os"
	"regexp"
	"strings"
)

var re *regexp.Regexp = regexp.MustCompile("[#].*\\n|\\s+\\n|\\S+[=]|.*\n")

func LoadConfig(filename string, dest map[string]string) error {
	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	buff := make([]byte, fi.Size())
	f.Read(buff)
	f.Close()
	str := string(buff)
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	s2 := re.FindAllString(str, -1)

	for i := 0; i < len(s2); {
		if strings.HasPrefix(s2[i], "#") {
			i++
		} else if strings.HasSuffix(s2[i], "=") {
			key := strings.ToLower(s2[i])[0 : len(s2[i])-1]
			i++
			if strings.HasSuffix(s2[i], "\n") {
				val := strings.TrimSuffix(s2[i][0:len(s2[i])-1], "\r")
				i++
				dest[key] = val
			}
		} else if strings.Contains(" \t\r\n", s2[i][0:1]) {
			i++
		} else {
			return errors.New(`error in config near: "` + s2[i])
		}
	}
	return nil
}
