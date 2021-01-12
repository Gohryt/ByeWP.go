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
	"sync"
	"time"
)

type (
	Global struct {
		Flags struct {
			Make         *bool
			Ask          *bool
			PostsFrom    *int64
			PostsFromEnd *bool
			UsersFrom    *int64
			UsersFromEnd *bool
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
	UserNew struct {
		Id       int64
		Username string
		Email    string
	}
	UserOld struct {
		ID                int64
		UserLogin         string
		UserPass          string
		UserNicename      string
		UserEmail         string
		UserUrl           string
		UserRegistered    time.Time
		UserActivationKey string
		UserStatus        int64
		DisplayName       string
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
		global    Global
		err       error
		waitGroup sync.WaitGroup
	)

	global.Flags.Make = flag.Bool("make", false, "make database")
	global.Flags.Ask = flag.Bool("ask", false, "ask for short posts")
	global.Flags.PostsFrom = flag.Int64("postsFrom", 1, "posts start from ...")
	global.Flags.PostsFromEnd = flag.Bool("postsFromEnd", false, "posts start from end")
	global.Flags.UsersFrom = flag.Int64("usersFrom", 1, "users start from ...")
	global.Flags.UsersFromEnd = flag.Bool("usersFromEnd", false, "users start from end")
	flag.Parse()

	fmt.Printf("Program started with params: make = %v, ask - %v, postsFrom - %v, postsFromEnd - %v, usersFrom - %v, usersFromEnd - %v\n", *global.Flags.Make, *global.Flags.Ask, *global.Flags.PostsFrom, *global.Flags.PostsFromEnd, *global.Flags.UsersFrom, *global.Flags.UsersFromEnd)

	global.Databases.Old.FromFile("old.json", gmanager.CriticalHandler)
	global.Databases.New.FromFile("new.json", gmanager.CriticalHandler)
	global.Replacers.WordPressComment.FromFile("wpc.json", gmanager.CriticalHandler)
	global.Replacers.WordPressSpacers.FromFile("wps.json", gmanager.CriticalHandler)
	global.Replacers.WordPressMedia.FromFile("wpm.json", gmanager.CriticalHandler)

	waitGroup.Add(2)
	fmt.Printf("Databases:\n")
	go func() {
		global.Connections.Old, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.Old.User, global.Databases.Old.Password, global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name))
		gmanager.CriticalHandler(&err)
		if global.Connections.Old != nil {
			fmt.Printf("Old - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name, global.Databases.Old.User, global.Databases.Old.Password)
			global.Connections.Old.SetConnMaxLifetime(time.Second * 10)
		} else {
			err := errors.New("old database not connected")
			gmanager.CriticalHandler(&err)
		}
		waitGroup.Done()
	}()
	go func() {
		global.Connections.New, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.New.User, global.Databases.New.Password, global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name))
		gmanager.CriticalHandler(&err)
		if global.Connections.New != nil {
			fmt.Printf("New - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name, global.Databases.New.User, global.Databases.New.Password)
			global.Connections.New.SetConnMaxLifetime(time.Second * 10)
		} else {
			err := errors.New("new database not connected")
			gmanager.CriticalHandler(&err)
		}
		waitGroup.Done()
	}()
	waitGroup.Wait()

	if *global.Flags.Make {
		_, err = global.Connections.New.Exec("create table LegacyUsers (Id int primary key, Username varchar(128) not null default '', Email varchar(512) not null default '');")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyTags (Id int primary key, Tag varchar(64) not null default '');")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyPosts (Id int primary key, Author int not null default 0, Date bigint not null default 0, Image varchar(512) not null default '', Title varchar(256) not null default '', Content longtext not null default '', foreign key (Author) references LegacyUsers (Id));")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyDependencies (PostId int not null default 0, TagId int not null default 0, foreign key (PostId) references LegacyPosts (Id), foreign key (TagId) references LegacyTags (Id));")
		gmanager.CriticalHandler(&err)
	}

	waitGroup.Add(2)
	go func() {
		if *global.Flags.UsersFromEnd && !*global.Flags.Make {
			var (
				row     *sql.Row
				lastSql sql.NullInt64
			)
			row = global.Connections.New.QueryRow("select max(Id) from LegacyUsers")
			gmanager.CriticalHandler(&err)
			if row != nil {
				err = row.Scan(&lastSql)
				gmanager.CriticalHandler(&err)
				if lastSql.Valid {
					fmt.Printf("Last migrated user is %d, we will start from it\n", lastSql.Int64)
					lastSql.Int64++
					global.Flags.UsersFrom = &lastSql.Int64
				} else {
					fmt.Printf("Migrated users were not found\n")
				}
			} else {
				err := errors.New("getting last migrated user was unsuccessful")
				gmanager.CriticalHandler(&err)
			}
		}
		waitGroup.Done()
	}()
	go func() {
		if *global.Flags.PostsFromEnd && !*global.Flags.Make {
			var (
				row     *sql.Row
				lastSql sql.NullInt64
			)
			row = global.Connections.New.QueryRow("select max(Id) from LegacyPosts")
			gmanager.CriticalHandler(&err)
			if row != nil {
				err = row.Scan(&lastSql)
				gmanager.CriticalHandler(&err)
				if lastSql.Valid {
					fmt.Printf("Last migrated post is %d, we will start from it\n", lastSql.Int64)
					lastSql.Int64++
					global.Flags.PostsFrom = &lastSql.Int64
				} else {
					fmt.Printf("Migrated posts were not found\n")
				}
			} else {
				err := errors.New("getting last migrated post was unsuccessful")
				gmanager.CriticalHandler(&err)
			}
		}
		waitGroup.Done()
	}()
	waitGroup.Wait()

	var (
		maxUser sql.NullInt64
		//maxPost sql.NullInt64
		row *sql.Row
	)

	row = global.Connections.Old.QueryRow("select max(ID) from wp_users")
	if row != nil {
		err = row.Scan(&maxUser)
		gmanager.CriticalHandler(&err)
		if !maxUser.Valid {
			fmt.Printf("No users found, users will not be migrated\n")
		}
	}
	if maxUser.Valid {
		var (
			row     *sql.Row
			userOld UserOld
			userNew UserNew
		)
		for *global.Flags.UsersFrom <= maxUser.Int64 {
			row = global.Connections.Old.QueryRow("select * from wp_users where ID = ?", *global.Flags.UsersFrom)
			if row != nil {
				if !userOld.scan(row) {
					userNew = *userOld.new()
					_, err = global.Connections.New.Exec("insert into LegacyUsers (Id, Username, Email) values (?, ?, ?)", userNew.Id, userNew.Username, userNew.Email)
					gmanager.CriticalHandler(&err)
				}
			} else {
				err := errors.New("getting users was unsuccessful")
				gmanager.CriticalHandler(&err)
			}
			*global.Flags.UsersFrom++
		}
	}
	/*
		row = global.Connections.Old.QueryRow("select max(ID) from wp_posts")
		if row != nil {
			err = row.Scan(&maxPost)
			gmanager.CriticalHandler(&err)
			if !maxUser.Valid {
				fmt.Printf("No posts found, posts will not be migrated\n")
			}
		}
		if maxPost.Valid {
			for *global.Flags.PostsFrom <= maxPost.Int64 {
				print(*global.Flags.PostsFrom)
				*global.Flags.PostsFrom++
			}
		}

	*/

	err = global.Connections.Old.Close()
	gmanager.CriticalHandler(&err)
	err = global.Connections.New.Close()
	gmanager.CriticalHandler(&err)
	return
}

func (un *UserNew) print() {
	fmt.Printf("Id: %v Username: %v Email: %v\n", un.Id, un.Username, un.Email)
	return
}

func (un *UserNew) scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&un.Id,
		&un.Username,
		&un.Email,
	)
	if err != sql.ErrNoRows {
		gmanager.CriticalHandler(&err)
	} else {
		noRows = true
	}
	return
}

func (uo *UserOld) print() {
	fmt.Printf("Id: %v Username: %v Email: %v\n", uo.ID, uo.UserNicename, uo.UserEmail)
	return
}

func (uo *UserOld) scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&uo.ID,
		&uo.UserLogin,
		&uo.UserPass,
		&uo.UserNicename,
		&uo.UserEmail,
		&uo.UserUrl,
		&uo.UserRegistered,
		&uo.UserActivationKey,
		&uo.UserStatus,
		&uo.DisplayName,
	)
	if err != sql.ErrNoRows {
		gmanager.CriticalHandler(&err)
	} else {
		noRows = true
	}
	return
}

func (uo *UserOld) new() (un *UserNew) {
	un = &UserNew{
		Id:       uo.ID,
		Username: uo.UserNicename,
		Email:    uo.UserEmail,
	}
	return
}

func (pn *PostNew) print() {
	fmt.Printf("Id: %v Author: %v Date: %v\nTitle: %v\nImage: %v\nContent: %v\n", pn.Id, pn.Author, time.Unix(pn.Date, 0).String(), pn.Title, pn.Image, pn.Content)
	return
}

func (pn *PostNew) scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&pn.Id,
		&pn.Author,
		&pn.Date,
		&pn.Image,
		&pn.Title,
		&pn.Content,
	)
	if err != sql.ErrNoRows {
		gmanager.CriticalHandler(&err)
	} else {
		noRows = true
	}
	return
}

func (po *PostOld) print() {
	fmt.Printf("Id: %v Author: %v Date: %v\nTitle: %v\nContent: %v\n", po.ID, po.PostAuthor, po.PostDate.String(), po.PostTitle, po.PostContent)
	return
}

func (po *PostOld) scan(row *sql.Row) (noRows bool) {
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
	if err != sql.ErrNoRows {
		gmanager.CriticalHandler(&err)
	} else {
		noRows = true
	}
	return
}

func (po *PostOld) new(global Global) (pn *PostNew) {
	var (
		content = po.PostContent
	)
	global.Replacers.WordPressComment.Replace(&content)
	global.Replacers.WordPressSpacers.Replace(&content)
	global.Replacers.WordPressMedia.Replace(&content)
	pn = &PostNew{
		Id:      po.ID,
		Author:  po.PostAuthor,
		Date:    po.PostDate.Unix(),
		Title:   po.PostTitle,
		Content: content,
	}
	return
}
