package design

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"github.com/Vivino/rankdb"
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = Resource("lists", func() {
	BasePath("/lists")
	Action("create", func() {
		Metadata("swagger:summary", "Create New List")
		Description("Create New List.\nIf the list already exists \"Conflict\" will be returned.\n" +
			"The provided populated data may be provided unsorted, but should not contain duplicate element IDs.")
		Routing(
			POST(""),
		)
		Params(func() {
			Param("replace", Boolean, func() {
				Description("Replace list if exists.")
				Default(false)
			})
		})
		Payload(RankListPayload)
		Response(OK, func() {
			Media(RankList, "full")
		})
		Response(Conflict, ErrorMedia)
		Response(BadRequest, ErrorMedia)
		JWTAPIManage()
	})
	Action("reindex", func() {
		Metadata("swagger:summary", "Reindex list")
		Description("Recreates ID index on entire list")
		Routing(
			PUT(":list_id/reindex"),
		)
		Params(func() {
			ListID("list_id")
		})
		JWTAPIManage()
		Response(OK, func() {
			Media(RankList, "full")
		})
		Response(NotFound, ErrorMedia)
		Response(BadRequest, ErrorMedia)
	})
	Action("repair", func() {
		Metadata("swagger:summary", "Repair list")
		Description("Repairs the list, by recreating all segments and indexes.\nAll access to the list is blocked while operation runs.")
		Routing(
			PUT(":list_id/repair"),
		)
		Params(func() {
			ListID("list_id")
			Param("clear", Boolean, func() {
				Description("Clear list if unable to repair")
				Default(false)
			})
		})
		JWTAPIManage()
		Response(OK, func() {
			Media(ListOpResult, "default")
		})
		Response(NotFound, ErrorMedia)
		Response(BadRequest, ErrorMedia)
	})
	Action("verify", func() {
		Metadata("swagger:summary", "Verify list")
		Description("Verify the list and its elements and optionally repair it.\nAll write access to the list is blocked while operation runs.")
		Routing(
			PUT(":list_id/verify"),
		)
		Params(func() {
			ListID("list_id")
			Param("repair", Boolean, func() {
				Description("Attempt to repair list")
				Default(false)
			})
			Param("clear", Boolean, func() {
				Description("Clear list if unable to repair")
				Default(false)
			})
		})
		JWTAPIManage()
		Response(OK, func() {
			Media(ListOpResult, "default")
		})
		Response(NotFound, ErrorMedia)
		Response(BadRequest, ErrorMedia)
	})
	Action("clone", func() {
		Metadata("swagger:summary", "Clone list")
		Description("Creates a clone of the list to a new list with the supplied metadata.\n" +
			"The URL list is the source and the payload must contain the new list ID.")
		Routing(
			PUT(":list_id/clone"),
		)
		Params(func() {
			ListID("list_id")
		})
		Payload(RankListPayload)
		JWTAPIManage()
		Response(OK, func() {
			Media(RankList, "full")
		})
		Response(NotFound, ErrorMedia, func() {
			Description("Returned if the original list cannot be found")
		})
		Response(Conflict, ErrorMedia, func() {
			Description("Returned if the new list already exists.")
		})
		Response(BadRequest, ErrorMedia)
	})
	Action("get_all", func() {
		Metadata("swagger:summary", "Get Multiple Lists")
		Description("Get multiple lists.\n" +
			"Lists are sorted lexicographically. See https://golang.org/pkg/strings/#Compare")
		Routing(
			GET(""),
		)
		Params(func() {
			pageParams()
		})
		Response(OK, ListsResult)
		Response(BadRequest, ErrorMedia)
		JWTAPIManage()
	})
	Action("get", func() {
		Routing(
			GET(":list_id"),
		)
		Params(func() {
			ListID("list_id")
			IncludeTopBottom()
		})
		Metadata("swagger:summary", "Return Single List")
		Description("Return Single List.\n" +
			"Note that top and bottom element will be omitted on empty lists.")
		Response(OK, func() {
			Media(RankList, "full")
		})
		Response(NotFound, ErrorMedia)
		Response(BadRequest, ErrorMedia)
		JWTAPIRead()
	})
	Action("get-range", func() {
		Metadata("swagger:summary", "Get rank range of the list")
		Description("Get rank range of the list.\n" +
			"Either `from_top` or `from_bottom` must be supplied")
		Routing(
			GET(":list_id/range"),
		)
		Params(func() {
			ListID("list_id")
			Param("limit", Integer, func() {
				Description("Number of results to return")
				Minimum(1)
				Maximum(1000)
				Default(25)
			})
			Param("from_top", Integer, func() {
				Description("First result will be at this rank from the top of the list.")
				Minimum(0)
			})
			Param("from_bottom", Integer, func() {
				Description("First result will be at this rank from the bottom of the list.")
				Minimum(0)
			})
		})
		Response(OK, CollectionOf(ElementRank, func() {
			View("default")
		}), "Elements above the current element.")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		JWTAPIRead()
	})
	Action("get-percentile", func() {
		Metadata("swagger:summary", "Get element at percentile")
		Description("Get element at percentile.\n" +
			"Either `from_top` or `from_bottom` must be supplied")
		Routing(
			GET(":list_id/percentile"),
		)
		Params(func() {
			ListID("list_id")
			rangeParam()
			Param("from_top", String, func() {
				Description("Return median percentile element.\n" +
					"If the percentile is between two elements, the element with the highest score is returned.\n" +
					"Value must be parseable as a float point number and must be between 0.0 and 100.0")
				Example("50.0")
				Default("50.0")
			})
		})
		Response(OK, func() {
			Media(ElementRank, "full")
		})
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		JWTAPIRead()
	})
	Action("delete", func() {
		Routing(
			DELETE(":list_id"),
		)
		Params(func() {
			ListID("list_id")
		})
		Description("Delete List.\n" +
			"Also returns success if list was not found.")
		Metadata("swagger:summary", "Delete List")
		Response(NoContent)
		Response(BadRequest, ErrorMedia)
		JWTAPIManage()
	})
})

var pageParams = func() {
	Param("after_id", String, func() {
		Description("Start with element following this ID. Empty will return from start.")
		Default("")
		Example("list")
	})
	Param("before_id", String, func() {
		Description("Return elements preceding this ID.")
		Default("")
	})
	Param("limit", Integer, func() {
		Description("Maximum Number of results")
		Minimum(1)
		Maximum(1000)
		Default(25)
		Example(10)
	})
}

var ListID = func(name string) {
	Param(name, String, func() {
		Description("The ID of the list to apply the operation on.\nCan be `a` to `z` (both upper/lower case), `0` to `9` or one of these characters `_-.`")
		MinLength(2)
		MaxLength(100)
		Example("highscore-list")
		Pattern(`^[a-zA-Z0-9-_.]+$`)
	})
}

var IncludeTopBottom = func() {
	Param("top_bottom", Boolean, func() {
		Description("Include top_element and bottom_element in result.")
		Default(false)
	})
}

// RankListPayload defines the data structure used in the create bottle request body.
// It is also the base type for the bottle media type used to render bottles.
var RankListPayload = Type("RankList", func() {
	ListID("id")
	Attribute("set", String, func() {
		Description("Set used for storage")
		MinLength(2)
		MaxLength(100)
		Example("storage-set")
	})
	Attribute("metadata", HashOf(String, String), func() {
		Description("Custom metadata. String to String hash.")
		Example(mss{"game": "2", "country": "dk"})
	})
	Attribute("split_size", Integer, func() {
		Description("Split Segments larger than this number of entries")
		Minimum(10)
		Maximum(1e5)
		Default(2000)
		Example(2000)
	})
	Attribute("merge_size", Integer, func() {
		Description("Merge adjacent Segments with less than this number of entries")
		Minimum(10)
		Maximum(1e5)
		Default(500)
		Example(500)
	})
	Attribute("load_index", Boolean, func() {
		Description("Load Index on server startup")
		Default(false)
	})
	Attribute("populate", ArrayOf(ElementPayload), func() {
		Description("Populate list with specified elements")
	})
	Required("id", "set")
})

// RankList is the rank list resource media type.
var RankList = MediaType("application/vnd.rankdb.ranklist+json", func() {
	Description("Rank List")
	Reference(RankListPayload)
	Attributes(func() {
		Attribute("id")
		Attribute("set")
		Attribute("split_size")
		Attribute("merge_size")
		Attribute("load_index")
		Attribute("metadata")
		Attribute("elements", Integer, func() {
			Description("Number of elements in list")
			Example(15000)
		})
		Attribute("top_element", ElementRank, func() {
			Description("This element is only returned if 'top_bottom' parameter is set to true.")
		})
		Attribute("bottom_element", ElementRank)
		Attribute("segments", Integer, func() {
			Description("Number of segment in list")
			Example(15)
		})
		Attribute("avg_segment_elements", Number, func() {
			Description("Average number of elements per segment")
			Example(1005.5)
		})
		Attribute("cache_hits", Integer, func() {
			Description("Cache hits while segments have been loaded.")
			Example(643)
		})
		Attribute("cache_misses", Integer, func() {
			Description("Cache misses while segments have been loaded.")
			Example(123)
		})
		Attribute("cache_percent", Number, func() {
			Description("Cache hit percentage while segments have been loaded.")
			Example(64.56)
		})
	})
	Required("id")
	Required("set", "metadata")
	Required("segments", "elements", "avg_segment_elements")
	Required("split_size", "merge_size", "load_index")
	Required("cache_hits", "cache_misses", "cache_percent")

	tiny := func() {
		Attribute("id")
	}
	View("tiny", func() {
		tiny()
	})

	def := func() {
		tiny()
		Attribute("set")
		Attribute("metadata")
		Attribute("split_size")
		Attribute("merge_size")
		Attribute("load_index")
	}
	View("default", func() {
		def()
	})

	View("full", func() {
		def()
		Attribute("segments")
		Attribute("avg_segment_elements")
		Attribute("elements")
		Attribute("top_element")
		Attribute("bottom_element")
		Attribute("cache_hits")
		Attribute("cache_misses")
		Attribute("cache_percent")
	})
})

var ListsResult = MediaType("application/vnd.rankdb.listsresult+json", func() {
	Attribute("lists_before", Integer, func() {
		Description("The number of lists before the first element")
	})
	Attribute("lists_after", Integer, func() {
		Description("The number of lists after the last element")
	})
	Attribute("lists", CollectionOf(RankList, func() {
		View("default")
		View("tiny")
	}))
	Required("lists_before", "lists_after", "lists")
	View("default", func() {
		Attribute("lists_before")
		Attribute("lists_after")
		Attribute("lists")
	})
})

var ListOpResult = MediaType("application/vnd.rankdb.listopresult+json", func() {
	Description("Result of a list operation. Will contain either an error or a list.")
	Attributes(func() {
		Attribute("error", String, func() {
			Description("Error, if any encountered")
			Example(rankdb.ErrNotFound.Error())
		})
		Attribute("list", RankList, func() {
			View("default")
		})

	})

	View("default", func() {
		Attribute("error")
		Attribute("list")
	})
})
