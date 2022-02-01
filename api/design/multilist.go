package design

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"encoding/json"

	"github.com/Vivino/rankdb"
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = Resource("multilist", func() {
	Description("Cross-list operations")
	BasePath("/xlist")
	Action("create", func() {
		Routing(
			POST("elements"),
		)
		JWTAPIUpdate()
		Params(func() {
			withResultsParam()
			withErrorsOnly()
		})
		Payload(ListPayloadQL)
		Description("Create elements in lists")
		Metadata("swagger:summary", "Create elements in multiple lists")
		Response(OK, func() {
			Media(ResultList, "default")
		})
		Response(BadRequest, ErrorMedia)
	})
	Action("delete", func() {
		Routing(
			DELETE("elements/:element_id"),
		)
		JWTAPIDelete()
		Params(func() {
			elementID()
			paramQL()
			withErrorsOnly()
		})
		Metadata("swagger:summary", "Delete Element in multiple lists")
		Description("Delete element in multiple lists.\n" +
			"If element is not found the operation is considered a success")
		Response(OK, ResultList)
		Response(BadRequest, ErrorMedia)
	})
	Action("get", func() {
		Routing(
			GET("elements/:element_id"),
		)
		JWTAPIRead()
		Params(func() {
			elementID()
			paramQL()
		})
		Description("Get Element in multiple lists")
		Metadata("swagger:summary", "Get Element in multiple lists")
		Response(OK, ResultList)
		Response(BadRequest, ErrorMedia)
	})
	Action("put", func() {
		Routing(
			PUT("elements"),
		)
		Params(func() {
			withResultsParam()
			withErrorsOnly()
		})
		JWTAPIUpdate()
		Payload(ListPayloadQL)
		Description("Update multiple elements in lists")
		Metadata("swagger:summary", "Update Elements in multiple lists"+
			"Results are always returned.")
		Response(OK, ResultList)
		Response(BadRequest, ErrorMedia)
	})
	Action("verify", func() {
		Routing(
			PUT("verify"),
		)
		Params(func() {
			Param("elements", Boolean, func() {
				Description("Verify elements as well")
				Default(false)
			})
			Param("repair", Boolean, func() {
				Description("Repair lists with problems automatically.")
				Default(false)
			})
			Param("clear", Boolean, func() {
				Description("Clear list if unable to repair")
				Default(false)
			})
			withErrorsOnly()
		})
		JWTAPIManage()
		Payload(ListQL)
		Description("Verify lists.\n" +
			"If no lists, nor any search is specified all lists are checked.")
		Metadata("swagger:summary", "Verify lists")
		Response(OK, ResultList)
		Response(BadRequest, ErrorMedia)
	})
	Action("reindex", func() {
		Routing(
			PUT("reindex"),
		)
		Params(func() {
			withErrorsOnly()
		})
		JWTAPIManage()
		Payload(ListQL)
		Description("Reindex lists.\n" +
			"If no lists, nor any search is specified all lists are reindexed.")
		Metadata("swagger:summary", "Reindex lists")
		Response(OK, ResultList)
		Response(BadRequest, ErrorMedia)
	})
	Action("backup", func() {
		Routing(
			PUT("backup"),
		)
		JWTAPIManage()
		Payload(MultiListBackup)
		Description("Backup lists.\n" +
			"If no lists, nor any search is specified all lists are backed up." +
			"A callback is provided to check progress.")
		Metadata("swagger:summary", "Backup lists")
		Response(Created, Callback, func() {
			Description("Returned when the operation is started in the background")
		})
		Response(OK, "application/octet-stream", func() {
			Description(`Returned when backup type is "download". The raw backup content is returned`)
		})
		Response(BadRequest, ErrorMedia)
	})
	Action("restore", func() {
		Routing(
			POST("restore"),
		)
		Params(func() {
			Param("list_id_suffix", String, func() {
				Description("Optional alternative list id suffix.\n" +
					"If not provided the original list id/segment ids will be used and any existing list will be overwritten.")
				Example("-restored")
			})
			Param("list_id_prefix", String, func() {
				Description("Optional alternative list id prefix.\n" +
					"If not provided the original list id/segment ids will be used and any existing list will be overwritten.")
				Example("restored-")
			})
			Param("set_prefix", String, func() {
				Description("Optional alternative prefix for the set.\n" +
					"If not provided the original set for the list will be used.")
				Example("namespace-")
			})
			Param("keep", Boolean, func() {
				Description("Keep existing lists. Only restore missing lists")
				Default(false)
			})
			Param("src", String, func() {
				Description("The body will not contain any data. Instead load data from provided URL.\n" +
					"The call will return when the backup has finished.\n" +
					"If the source is s3, the source should be defined as s3://bucket/path/file.bin. Replace bucket and path+file")
				Default("")
				Example("http://10.0.0.1:8080/restoredata")
			})
			Param("src_file", String, func() {
				Description("The body will not contain any data. Instead load data from file from this path.\n" +
					"The call will return when the backup has finished.")
				Default("")
				Example("/tmp/ckoreN172h67Dy3cQ4.bin")
			})
		})
		JWTAPIManage()
		Description("Restore lists. Body must contain binary data with backup data, unless 'src' is specified.")
		Metadata("swagger:summary", "Restore lists")
		Response(OK, RestoreResultList)
		Response(BadRequest, ErrorMedia)
	})
})

// ListPayloadQL defines the data structure used in the create element request body.
var ListPayloadQL = Type("ListPayloadQL", func() {
	listQL()
	Attribute("payload", ArrayOf(ElementPayload), func() {
		Description("Payloads for create/update functions.\n" + "" +
			"Will be ignored on deletes.")
	})
})

var BackupDestination = Type("BackupDestination", func() {
	Param("type", String, func() {
		Description("Specifies the destination type of the backup. Can be 'file' 'download', 's3' or 'server'")
		Example("file")
	})
	Param("path", String, func() {
		Description("Specifies the destination path.\n" +
			"If type is server, this should be ip+port of the other server, eg. 10.0.0.1:8080.\n" +
			"If the type is s3, the source should be specified as s3://bucket/path/file.bin. Replace bucket and path+file",
		)
		Example("./backup.bin")
	})
	Param("server_list_id_suffix", String, func() {
		Description("Optional alternative list id suffix for server-to-server transfers.\n" +
			"If not provided the original list id/segment ids will be used and any existing list will be overwritten.")
	})
	Param("server_list_id_prefix", String, func() {
		Description("Optional alternative list id prefix for server-to-server transfers.\n" +
			"If not provided the original list id/segment ids will be used and any existing list will be overwritten.")
	})
	Required("type")
})

// MultiListBackup defines the query types to access multiple lists
var MultiListBackup = Type("MultiListBackup", func() {
	Param("destination", BackupDestination)
	Param("lists", ListQL)
	Required("destination", "lists")
})

// ListQL defines the query types to access multiple lists
var ListQL = Type("ListQL", func() {
	listQL()
})

var listQL = func() {
	Attribute("lists", ArrayOf(String), func() {
		Description("Include lists that match exact list names")
		Example([]string{"highscore-dk-all", "highscore-uk-all"})
	})
	Attribute("match_metadata", HashOf(String, String), func() {
		Description("Include lists that match all values in metadata")
		Example(mss{"country": "dk"})
	})
	Attribute("all_in_sets", ArrayOf(String), func() {
		Description("Include all lists in these sets")
		Example([]string{"storage-set", "backup-set"})
	})
}

func withErrorsOnly() {
	Param("errors_only", Boolean, func() {
		Description("Returns errors only. If disabled, operations will be faster and require less memory.")
		Default(false)
	})
}

var paramQL = func() {
	Attribute("lists", ArrayOf(String), func() {
		Description("Include lists that match exact list names")
		Example([]string{"highscore-dk-all", "highscore-uk-all"})
	})
	Attribute("match_metadata", String, func() {
		ex, err := json.Marshal(mss{"country": "dk", "game": "match4"})
		if err != nil {
			panic(err)
		}
		Description("Include lists that match all values in metadata.\n" +
			"Payload must be valid json with string->string values.\n" +
			"Example: " + string(ex))
		Example(string(ex))
	})
	Attribute("all_in_sets", ArrayOf(String), func() {
		Description("Include all lists in these sets")
		Example([]string{"storage-set", "backup-set"})
	})
}
var OperationSuccess = MediaType("application/vnd.rankdb.operation_success+json", func() {
	Attribute("results", CollectionOf(ElementRank), func() {
		Description("If `results` parameter was true, the resulting element is returned here.")
		View("tiny")
	})
	View("default", func() {
		Attribute("results")
	})
})

var Callback = MediaType("application/vnd.rankdb.callback+json", func() {
	Description("Backup Information")
	Attributes(func() {
		Attribute("id", String)
		Attribute("callback_url", String)
	})
	Required("id", "callback_url")
	View("default", func() {
		Attribute("id")
		Attribute("callback_url")
	})
})

var ResultList = MediaType("application/vnd.rankdb.resultlist+json", func() {
	Attributes(func() {
		Attribute("success", HashOf(String, OperationSuccess), func() {
			Description("Successful operations, indexed by list IDs")
			Example(msi{"highscore-uk-all": msi{}})
		})
		Attribute("errors", HashOf(String, String), func() {
			Description("Failed operations, indexed by list IDs")
			Example(msi{"highscore-dk-all": rankdb.ErrNotFound.Error()})
		})
	})
	View("default", func() {
		Attribute("success")
		Attribute("errors")
		Required("success", "errors")
	})
})

var RestoreResultList = MediaType("application/vnd.rankdb.restoreresult+json", func() {
	Attributes(func() {
		Attribute("restored", Integer, func() {
			Description("Successful restore operations, indexed by list IDs")
			Example(20)
		})
		Attribute("skipped", Integer, func() {
			Description("Skipped lists, indexed by list IDs")
			Example(1)
		})
		Attribute("errors", HashOf(String, String), func() {
			Description("Failed operations, indexed by list IDs")
			Example(msi{"highscore-dk-all": rankdb.ErrNotFound.Error()})
		})
	})
	Required("restored", "skipped", "errors")
	View("default", func() {
		Attribute("restored")
		Attribute("skipped")
		Attribute("errors")
	})
})
