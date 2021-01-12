package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	gdatabase "github.com/Gohryt/Impossible.go/database"
	gmanager "github.com/Gohryt/Impossible.go/manager"
	gregexp "github.com/Gohryt/Impossible.go/regexp"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type (
	Global struct {
		Flags struct {
			Make    *bool
			Ask     *bool
			From    *int64
			FromEnd *bool
		}
		Databases struct {
			Old gdatabase.Configuration
			New gdatabase.Configuration
		}
		Connections struct {
			Old *sql.DB
			New *sql.DB
		}
		Replacers struct {
			WordPressComment gregexp.Expression
			WordPressSpacers gregexp.Expression
			WordPressMedia   gregexp.Expression
		}
	}
	PostNew struct {
		Id      int64
		Author  int64
		Date    int64
		Title   string
		Image   string
		Content string
	}
	PostOld struct {
		ID                  int64
		PostAuthor          int64
		PostDate            time.Time
		PostDateGmt         time.Time
		PostContent         string
		PostTitle           string
		PostExcerpt         string
		PostStatus          string
		CommentStatus       string
		PingStatus          string
		PostPassword        string
		PostName            string
		ToPing              string
		Pinged              string
		PostModified        time.Time
		PostModifiedGmt     time.Time
		PostContentFiltered string
		PostParent          int64
		Guid                string
		MenuOrder           int64
		PostType            string
		PostMimeType        string
		CommentCount        int64
	}
)

func main() {
	var (
		global     Global
		err        error
		selectLast *sql.Stmt
	)

	global.Flags.Make = flag.Bool("make", false, "make database")
	global.Flags.Ask = flag.Bool("ask", false, "ask for short posts")
	global.Flags.From = flag.Int64("from", 0, "start from ...")
	global.Flags.FromEnd = flag.Bool("fromEnd", false, "start from end")
	flag.Parse()

	fmt.Printf("Program started with params: make = %v, ask - %v, from - %v, fromEnd - %v\n", *global.Flags.Make, *global.Flags.Ask, *global.Flags.From, *global.Flags.FromEnd)

	global.Databases.Old.FromFile("old.json", gmanager.CriticalHandler)
	global.Databases.New.FromFile("new.json", gmanager.CriticalHandler)
	global.Replacers.WordPressComment.FromFile("wpc.json", gmanager.CriticalHandler)
	global.Replacers.WordPressSpacers.FromFile("wps.json", gmanager.CriticalHandler)
	global.Replacers.WordPressMedia.FromFile("wpm.json", gmanager.CriticalHandler)

	global.Connections.Old, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.Old.User, global.Databases.Old.Password, global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name))
	gmanager.CriticalHandler(&err)
	global.Connections.New, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.New.User, global.Databases.New.Password, global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name))
	gmanager.CriticalHandler(&err)

	fmt.Printf("Databases:\n")
	if global.Connections.Old != nil {
		fmt.Printf("Old - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name, global.Databases.Old.User, global.Databases.Old.Password)
		global.Connections.Old.SetConnMaxLifetime(time.Second * 10)
	} else {
		err := errors.New("old database not connected")
		gmanager.CriticalHandler(&err)
	}
	if global.Connections.New != nil {
		fmt.Printf("New - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name, global.Databases.New.User, global.Databases.New.Password)
		global.Connections.New.SetConnMaxLifetime(time.Second * 10)
	} else {
		err := errors.New("new database not connected")
		gmanager.CriticalHandler(&err)
	}

	selectLast, err = global.Connections.New.Prepare("select max(Id) from LegacyPosts")
	gmanager.CriticalHandler(&err)

	if *global.Flags.FromEnd {
		var (
			row  *sql.Row
			last int64
		)
		row = selectLast.QueryRow()
		gmanager.CriticalHandler(&err)
		if row != nil {
			err = row.Scan(&last)
			gmanager.CriticalHandler(&err)
			last++
			global.Flags.From = &last
			fmt.Printf("Last migrated post is %d, we will start from it\n", *global.Flags.From)
		}
	}

	err = global.Connections.Old.Close()
	gmanager.CriticalHandler(&err)
	err = global.Connections.New.Close()
	gmanager.CriticalHandler(&err)
}

func (pn *PostNew) print() {
	fmt.Printf("Id: %v Author: %v Date: %v\nTitle: %v\nImage: %v\nContent: %v\n", pn.Id, pn.Author, time.Unix(pn.Date, 0).String(), pn.Title, pn.Image, pn.Content)
}

func (pn *PostNew) scan(row *sql.Row) {
	err := row.Scan(
		&pn.Id,
		&pn.Author,
		&pn.Date,
		&pn.Image,
		&pn.Title,
		&pn.Content,
	)
	gmanager.CriticalHandler(&err)
}

func (po *PostOld) print() {
	fmt.Printf("Id: %v Author: %v Date: %v\nTitle: %v\nContent: %v\n\n", po.ID, po.PostAuthor, po.PostDate.String(), po.PostTitle, po.PostContent)
}

func (po *PostOld) scan(row *sql.Row) {
	err := row.Scan(
		&po.ID,
		&po.PostAuthor,
		&po.PostDate,
		&po.PostDateGmt,
		&po.PostContent,
		&po.PostTitle,
		&po.PostExcerpt,
		&po.PostStatus,
		&po.CommentStatus,
		&po.PingStatus,
		&po.PostPassword,
		&po.PostName,
		&po.ToPing,
		&po.Pinged,
		&po.PostModified,
		&po.PostModifiedGmt,
		&po.PostContentFiltered,
		&po.PostParent,
		&po.Guid,
		&po.MenuOrder,
		&po.PostType,
		&po.PostMimeType,
		&po.CommentCount,
	)
	gmanager.CriticalHandler(&err)
}

func (po *PostOld) new(global Global) (pn *PostNew) {
	var (
		content = po.PostContent
	)
	global.Replacers.WordPressComment.Replace(&content)
	global.Replacers.WordPressSpacers.Replace(&content)
	global.Replacers.WordPressMedia.Replace(&content)
	return &PostNew{
		Id:      po.ID,
		Author:  po.PostAuthor,
		Date:    po.PostDate.Unix(),
		Title:   po.PostTitle,
		Content: content,
	}
}
