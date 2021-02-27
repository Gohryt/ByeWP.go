package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	gdatabase "github.com/Gohryt/Impossible.go/database"
	gmanager "github.com/Gohryt/Impossible.go/manager"
	gregexp "github.com/Gohryt/Impossible.go/regexp"
	_ "github.com/go-sql-driver/mysql"
)

type (
	global struct {
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
	userNew struct {
		ID       int64
		Username string
		Email    string
	}
	userOld struct {
		ID                int64
		UserLogin         string
		UserPass          string
		UserNicename      string
		UserEmail         string
		UserURL           string
		UserRegistered    time.Time
		UserActivationKey string
		UserStatus        int64
		DisplayName       string
	}
	postNew struct {
		ID      int64
		Author  int64
		Date    int64
		Title   string
		Image   string
		Content string
	}
	postOld struct {
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
		GUID                string
		MenuOrder           int64
		PostType            string
		PostMimeType        string
		CommentCount        int64
	}
)

func main() {
	var (
		global    global
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

	fmt.Printf("Databases:\n")
	global.Connections.Old, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.Old.User, global.Databases.Old.Password, global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name))
	gmanager.StandardHandler(&err)
	if global.Connections.Old != nil {
		fmt.Printf("Old - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.Old.Host, global.Databases.Old.Port, global.Databases.Old.Name, global.Databases.Old.User, hidePassword(global.Databases.Old.Password))
		global.Connections.Old.SetConnMaxLifetime(time.Second * 10)
	} else {
		err := errors.New("old database not connected")
		gmanager.CriticalHandler(&err)
	}
	global.Connections.New, err = sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true", global.Databases.New.User, global.Databases.New.Password, global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name))
	gmanager.StandardHandler(&err)
	if global.Connections.New != nil {
		fmt.Printf("New - Address: %v:%v Table: %v User: %v Password: %v\n", global.Databases.New.Host, global.Databases.New.Port, global.Databases.New.Name, global.Databases.New.User, hidePassword(global.Databases.New.Password))
		global.Connections.New.SetConnMaxLifetime(time.Second * 10)
	} else {
		err := errors.New("new database not connected")
		gmanager.CriticalHandler(&err)
	}

	if *global.Flags.Make {
		_, err = global.Connections.New.Exec("create table LegacyUsers (Id int primary key, Username varchar(128) not null default '', Email varchar(512) not null default '');")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyTags (Id int primary key, Tag varchar(64) not null default '');")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyPosts (Id int primary key, Author int not null default 0, Date bigint not null default 0, Title varchar(256) not null default '', Image varchar(512) not null default '', Content longtext not null default '', foreign key (Author) references LegacyUsers (Id));")
		gmanager.CriticalHandler(&err)
		_, err = global.Connections.New.Exec("create table LegacyDependencies (PostId int not null default 0, TagId int not null default 0, foreign key (PostId) references LegacyPosts (Id), foreign key (TagId) references LegacyTags (Id));")
		gmanager.CriticalHandler(&err)
	}

	waitGroup.Add(2)
	go func() {
		if *global.Flags.UsersFromEnd && !*global.Flags.Make {
			var (
				row     *sql.Row
				lastSQL sql.NullInt64
			)
			row = global.Connections.New.QueryRow("select max(Id) from LegacyUsers")
			gmanager.CriticalHandler(&err)
			if row != nil {
				err = row.Scan(&lastSQL)
				gmanager.CriticalHandler(&err)
				if lastSQL.Valid {
					fmt.Printf("Last migrated user is %d, we will start from it\n", lastSQL.Int64)
					lastSQL.Int64++
					global.Flags.UsersFrom = &lastSQL.Int64
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
				lastSQL sql.NullInt64
			)
			row = global.Connections.New.QueryRow("select max(Id) from LegacyPosts")
			gmanager.CriticalHandler(&err)
			if row != nil {
				err = row.Scan(&lastSQL)
				gmanager.CriticalHandler(&err)
				if lastSQL.Valid {
					fmt.Printf("Last migrated post is %d, we will start from it\n", lastSQL.Int64)
					lastSQL.Int64++
					global.Flags.PostsFrom = &lastSQL.Int64
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
		maxPost sql.NullInt64
		row     *sql.Row
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
			userOld userOld
			userNew userNew
		)
		for *global.Flags.UsersFrom <= maxUser.Int64 {
			row = global.Connections.Old.QueryRow("select * from wp_users where ID = ?", *global.Flags.UsersFrom)
			if row != nil {
				if !userOld.Scan(row) {
					userNew = *userOld.New()
					_, err = global.Connections.New.Exec("insert into LegacyUsers (Id, Username, Email) values (?, ?, ?)", userNew.ID, userNew.Username, userNew.Email)
					gmanager.CriticalHandler(&err)
				}
			} else {
				err := errors.New("getting users was unsuccessful")
				gmanager.CriticalHandler(&err)
			}
			*global.Flags.UsersFrom++
		}
	}
	row = global.Connections.Old.QueryRow("select max(ID) from wp_posts")
	if row != nil {
		err = row.Scan(&maxPost)
		gmanager.CriticalHandler(&err)
		if !maxUser.Valid {
			fmt.Printf("No posts found, posts will not be migrated\n")
		}
	}
	if maxPost.Valid {
		var (
			row            *sql.Row
			rows           *sql.Rows
			postOld        postOld
			postNew        postNew
			ask            bool
			scan           string
			add            bool
			termID         int
			isIt           *sql.Rows
			newID          int
			termName       *sql.Row
			termNameString string
			termType       *sql.Row
			termTaxonomy   string
			newImage       string
		)
		for *global.Flags.PostsFrom <= maxPost.Int64 {
			ask = false
			scan = ""
			add = true
			row = global.Connections.Old.QueryRow("select * from wp_posts where ID = ?", *global.Flags.PostsFrom)
			if row != nil {
				if !postOld.Scan(row) {
					if postOld.PostType != "post" && postOld.PostType != "revision" && postOld.PostType != "attachment" {
						add = false
					} else if len(postOld.PostContent) < 2048 && postOld.PostType != "attachment" {
						ask = true
					}
					if ask && add {
						postOld.Print()
						fmt.Printf("Do you want add this post (y|yes||n|no) ? ")
						_, err = fmt.Scan(&scan)
						if !(scan == "y" || scan == "yes") {
							add = false
						}
						gmanager.CriticalHandler(&err)
					}
					if add {
						switch postOld.PostType {
						case "post":
							postNew = *postOld.New(&global)
							_, err = global.Connections.New.Exec("insert into LegacyPosts (Id, Author, Date, Title, Content) values (?, ?, ?, ?, ?)", postNew.ID, postNew.Author, postNew.Date, postNew.Title, postNew.Content)
							gmanager.CriticalHandler(&err)
							rows, err = global.Connections.Old.Query("select term_taxonomy_id from wp_term_relationships where object_id = ?", postNew.ID)
							gmanager.CriticalHandler(&err)
							if rows != nil {
								for rows.Next() {
									termID = 0
									isIt = nil
									err = rows.Scan(&termID)
									gmanager.CriticalHandler(&err)
									isIt, err = global.Connections.New.Query("select Id from LegacyTags where Id = ?", termID)
									gmanager.CriticalHandler(&err)
									if isIt != nil {
										newID = 0
										for isIt.Next() {
											err = isIt.Scan(&newID)
											gmanager.CriticalHandler(&err)
										}
										if newID == 0 {
											termType = global.Connections.Old.QueryRow("select taxonomy from wp_term_taxonomy where term_id = ?", termID)
											termTaxonomy = ""
											err = termType.Scan(&termTaxonomy)
											gmanager.CriticalHandler(&err)
											if termTaxonomy == "post_tag" {
												termName = global.Connections.Old.QueryRow("select name from wp_terms where term_id = ?", termID)
												termNameString = ""
												err = termName.Scan(&termNameString)
												gmanager.CriticalHandler(&err)
												_, err = global.Connections.New.Exec("insert into LegacyTags (Id, Tag) values (?, ?)", termID, termNameString)
												gmanager.CriticalHandler(&err)
												_, err = global.Connections.New.Exec("insert into LegacyDependencies (PostId, TagId) values (?, ?)", postNew.ID, termID)
												gmanager.CriticalHandler(&err)
											}
										} else {
											termType = global.Connections.Old.QueryRow("select taxonomy from wp_term_taxonomy where term_id = ?", termID)
											termTaxonomy = ""
											err = termType.Scan(&termTaxonomy)
											gmanager.CriticalHandler(&err)
											if termTaxonomy == "post_tag" {
												_, err = global.Connections.New.Exec("insert into LegacyDependencies (PostId, TagId) values (?, ?)", postNew.ID, termID)
												gmanager.CriticalHandler(&err)
											}
										}
									}
								}
							}
						case "revision":
							postNew = *postOld.New(&global)
							_, err = global.Connections.New.Exec("update LegacyPosts set Title = ?, Content = ? where Id = ?", postNew.Title, postNew.Content, postOld.PostParent)
							gmanager.CriticalHandler(&err)
							rows, err = global.Connections.Old.Query("select term_taxonomy_id from wp_term_relationships where object_id = ?", postNew.ID)
							gmanager.CriticalHandler(&err)
							if rows != nil {
								for rows.Next() {
									termID = 0
									err = rows.Scan(&termID)
									gmanager.CriticalHandler(&err)
									isIt, err = global.Connections.New.Query("select Id from LegacyTags where Id = ?", termID)
									gmanager.CriticalHandler(&err)
									if isIt != nil {
										newID = 0
										for isIt.Next() {
											err = isIt.Scan(&newID)
											gmanager.CriticalHandler(&err)
										}
										if newID == 0 {
											termType = global.Connections.Old.QueryRow("select taxonomy from wp_term_taxonomy where term_id = ?", termID)
											termTaxonomy = ""
											err = termType.Scan(&termTaxonomy)
											gmanager.CriticalHandler(&err)
											if termTaxonomy == "post_tag" {
												termName = global.Connections.Old.QueryRow("select name from wp_terms where term_id = ?", termID)
												termNameString = ""
												err = termName.Scan(&termNameString)
												gmanager.CriticalHandler(&err)
												_, err = global.Connections.New.Exec("insert into LegacyTags (Id, Tag) values (?, ?)", termID, termNameString)
												gmanager.CriticalHandler(&err)
												_, err = global.Connections.New.Exec("insert into LegacyDependencies (PostId, TagId) values (?, ?)", postNew.ID, termID)
												gmanager.CriticalHandler(&err)
											}
										} else {
											termType = global.Connections.Old.QueryRow("select taxonomy from wp_term_taxonomy where term_id = ?", termID)
											termTaxonomy = ""
											err = termType.Scan(&termTaxonomy)
											gmanager.CriticalHandler(&err)
											if termTaxonomy == "post_tag" {
												_, err = global.Connections.New.Exec("insert into LegacyDependencies (PostId, TagId) values (?, ?)", postNew.ID, termID)
												gmanager.CriticalHandler(&err)
											}
										}
									}
								}
							}
						case "attachment":
							if postOld.GUID != "" {
								newImage = postOld.GUID
								global.Replacers.WordPressMedia.Replace(&newImage)
								_, err = global.Connections.New.Exec("update LegacyPosts set Image = ? where Id = ?", newImage, postOld.PostParent)
								gmanager.CriticalHandler(&err)
							}
						}
					}
				}
			} else {
				err := errors.New("getting users was unsuccessful")
				gmanager.CriticalHandler(&err)
			}
			*global.Flags.PostsFrom++
		}
	}

	err = global.Connections.Old.Close()
	gmanager.CriticalHandler(&err)
	err = global.Connections.New.Close()
	gmanager.CriticalHandler(&err)
	return
}

func (un *userNew) Print() {
	fmt.Printf("Id: %v Username: %v Email: %v\n", un.ID, un.Username, un.Email)
	return
}

func (un *userNew) Scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&un.ID,
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

func (uo *userOld) Print() {
	fmt.Printf("Id: %v Username: %v Email: %v\n", uo.ID, uo.UserNicename, uo.UserEmail)
	return
}

func (uo *userOld) Scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&uo.ID,
		&uo.UserLogin,
		&uo.UserPass,
		&uo.UserNicename,
		&uo.UserEmail,
		&uo.UserURL,
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

func (uo *userOld) New() (un *userNew) {
	un = &userNew{
		ID:       uo.ID,
		Username: uo.UserNicename,
		Email:    uo.UserEmail,
	}
	return
}

func (pn *postNew) Print() {
	fmt.Printf("Id: %v Author: %v Date: %v\nTitle: %v\nImage: %v\nContent: %v\n", pn.ID, pn.Author, time.Unix(pn.Date, 0).String(), pn.Title, pn.Image, pn.Content)
	return
}

func (pn *postNew) Scan(row *sql.Row) (noRows bool) {
	err := row.Scan(
		&pn.ID,
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

func (po *postOld) Print() {
	fmt.Printf("Id: %v Author: %v Date: %v Type: %v\nTitle: %v\nContent: %v\n", po.ID, po.PostAuthor, po.PostDate.String(), po.PostType, po.PostTitle, po.PostContent)
	return
}

func (po *postOld) Scan(row *sql.Row) (noRows bool) {
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
		&po.GUID,
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

func (po *postOld) New(global *global) (pn *postNew) {
	var (
		content = po.PostContent
	)
	global.Replacers.WordPressComment.Replace(&content)
	global.Replacers.WordPressSpacers.Replace(&content)
	global.Replacers.WordPressMedia.Replace(&content)
	pn = &postNew{
		ID:      po.ID,
		Author:  po.PostAuthor,
		Date:    po.PostDate.Unix(),
		Title:   po.PostTitle,
		Content: content,
	}
	return
}

func hidePassword(password string) string {
	var (
		i            int
		bytePassword []byte = []byte(password)
	)
	for i = range bytePassword {
		if i != 0 && i != (len(bytePassword)-1) {
			bytePassword[i] = '*'
		}
	}
	return string(bytePassword)
}
