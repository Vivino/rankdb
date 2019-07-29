package design

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"math"

	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = Resource("elements", func() {
	BasePath("/lists/:list_id/elements")
	ListID("list_id")
	Action("create", func() {
		Routing(
			POST(""),
		)
		Params(func() {
			rangeParam()
		})
		Payload(ElementPayload)
		Description("Create Element in list")
		Metadata("swagger:summary", "Create Element in list")
		Response(OK, func() {
			Media(ElementRank, "full")
		})
		Response(BadRequest, ErrorMedia)
		Response(Conflict, ErrorMedia)
		Response(NotFound, ErrorMedia)
		JWTAPIUpdate()
	})
	Action("get", func() {
		Routing(
			GET(":element_id"),
		)
		Params(func() {
			rangeParam()
			elementID()
		})
		Description("Get Element in list")
		Metadata("swagger:summary", "Get Element in list")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(OK, func() {
			Media(ElementRank, "full")
		})
		JWTAPIRead()
	})
	Action("get-multi", func() {
		Routing(
			POST("find"),
		)
		Payload(MultiElementPayload)
		Description("Get Multiple Elements in list.\n" +
			"Will return 404 if list cannot be found, OK even if no elements are found.")
		Metadata("swagger:summary", "Get Multiple Elements")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(OK, ElementRanks)
		JWTAPIRead()
	})
	Action("get-around", func() {
		Routing(
			POST(":element_id/around"),
		)
		Params(func() {
			rangeParam()
			elementID()
		})
		Description("Get relation of one element to multiple specific elements.\n" +
			"The element will have local_from_top and local_from_bottom populated." +
			"Elements that are not found are ignored.\n")
		Metadata("swagger:summary", "Get relation of one element to multiple specific elements.")
		Payload(MultiElementPayload)
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(OK, func() {
			Media(ElementRank, "full")
		})
		JWTAPIRead()
	})
	Action("put", func() {
		Routing(
			PUT(":element_id"),
		)
		Payload(ElementPayload)
		Params(func() {
			elementID()
			rangeParam()
		})
		Description("Update element in list\n" +
			"If element does not exist, it is created in list.\n" +
			"Element ID in payload an url must match.")
		Metadata("swagger:summary", "Update Element in list")
		Response(OK, func() {
			Media(ElementRank, "full-update")
		})
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		JWTAPIUpdate()
	})
	Action("put-multi", func() {
		Routing(
			PUT(""),
		)
		Payload(ArrayOf(ElementPayload))
		Params(func() {
			withResultsParam()
		})
		Description("Update Multiple Elements in list." +
			"If element does not exist, it is created in list.\n" +
			"The returned \"not_found\" field will never be preset.")
		Metadata("swagger:summary", "Update Multiple Elements")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(OK, ElementRanks)
		JWTAPIUpdate()
	})
	Action("delete", func() {
		Routing(
			DELETE(":element_id"),
		)
		Params(func() {
			elementID()
		})
		Metadata("swagger:summary", "Delete Element in list")
		Description("Delete element in list. If element is not found the operation is considered a success")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(NoContent)
		JWTAPIDelete()
	})
	Action("delete-multi", func() {
		Routing(
			DELETE(""),
		)
		Params(func() {
			elementIDs()
		})
		Description("Delete Multiple Elements in list." +
			"If an element does not exist, success is returned.\n")
		Metadata("swagger:summary", "Delete Multiple Elements in list")
		Response(BadRequest, ErrorMedia)
		Response(NotFound, ErrorMedia)
		Response(NoContent)
		JWTAPIDelete()
	})
})

var rangeParam = func() {
	Param("range", Integer, func() {
		Description("Return this number of elements above and below the current element in `neighbors` field.")
		Minimum(0)
		Maximum(100)
		Default(5)
	})
}

var elementID = func() {
	Param("element_id", String, func() {
		Description("ID of element")
		Example("100")
		MinLength(1)
		//Metadata("struct:field:type", "uint64")
		Pattern("^[0-9]+$")
		MaxLength(len(fmt.Sprint(uint64(math.MaxUint64))))
	})
}

// elementIDs defines element ids as attribute
var elementIDs = func() {
	Attribute("element_ids", ArrayOf(String, func() {
		MinLength(1)
		MaxLength(len(fmt.Sprint(uint64(math.MaxUint64))))
		Pattern("^[0-9]+$")
	}), func() {
		Description("IDs of elements")
		Example([]string{"101", "202", "303"})
		MinLength(1)
		MaxLength(10000)
	})
}

// MultiElementPayload defines the data structure for requesting multiple elements.
var MultiElementPayload = Type("MultiElement", func() {
	Attribute("element_ids", ArrayOf(Integer, func() {
		Description("Element ID")
		Example(100)
		Minimum(0)
		Metadata("struct:field:type", "uint64")
	}), func() {
		Description("IDs of elements")
		Example([]uint64{120, 340, 550})
		Metadata("struct:field:type", "[]uint64")
	})
	Required("element_ids")
})

// ElementPayload defines the data structure used in the create element request body.
var ElementPayload = Type("Element", func() {
	Attribute("id", Integer, func() {
		Description("ID of element")
		Example(100)
		Minimum(0)
		Metadata("struct:field:type", "uint64")
		//Maximum(uint64(math.MaxUint64))
	})
	Attribute("score", Integer, func() {
		Description("Score of the element. Higher score gives higher placement.")
		Example(100)
		Minimum(0)
		//Maximum(uint64(math.MaxUint64))
		Metadata("struct:field:type", "uint64")
	})
	Attribute("tie_breaker", Integer, func() {
		Description("Tie breaker, used if score matches for consistent sorting. Higher value = higher placement if score is equal.")
		Minimum(0)
		Maximum(math.MaxUint32)
		Example(2000)
		Metadata("struct:field:type", "uint32")
	})
	Attribute("payload", HashOf(String, Any), func() {
		Metadata("struct:field:type", "json.RawMessage", "encoding/json")
		Description("Custom payload. Stored untouched. On updates null means do not update. `{}` is the empty value.")
		Example(msi{"name": "Mark Anthony", "country": "dk"})
	})
	Required("id", "score")
})

func withResultsParam() {
	Param("results", Boolean, func() {
		Description("Return results of the operation. If disabled, operations will be faster and require less memory.")
		Default(false)
	})
}

// Element is an element on a rank list.
var ElementRank = MediaType("application/vnd.rankdb.element+json", func() {
	Description("List Element")
	Reference(ElementPayload)
	Attributes(func() {
		Attribute("id")
		Attribute("from_top", Integer, func() {
			Description("Element rank in list from top.\n Top element has value 0.")
			Example(50)
		})
		Attribute("from_bottom", Integer, func() {
			Description("Element rank in list from bottom.\n Bottom element has value 0.")
			Example(4000)
		})
		ListID("list_id")
		Attribute("score")
		Attribute("tie_breaker")
		Attribute("payload")
		Attribute("updated_at", DateTime, "Date of last update")
		Attribute("neighbors", func() {
			Description("Neighbors to current element")
			Attribute("above", CollectionOf("application/vnd.rankdb.element+json"), func() {
				View("default")
				Description("Elements above the current element.\nEnds with element just above current.")
			})
			Attribute("below", CollectionOf("application/vnd.rankdb.element+json"), func() {
				View("default")
				Description("Elements below the current element.\nStarts with element just below current.")
			})
		})
		Attribute("local_from_top", Integer, func() {
			Description("Local element rank in list from top when requesting sub-list.\n Top element has value 0.")
			Example(50)
		})
		Attribute("local_from_bottom", Integer, func() {
			Description("Local element rank in list from bottom when requesting sub-list.\n Bottom element has value 0.")
			Example(4000)
		})

		Attribute("previous_rank", "application/vnd.rankdb.element+json", "Rank of element before update")

		Required("id", "from_top", "from_bottom", "score")
		Required("list_id", "payload", "updated_at", "tie_breaker")
	})

	tiny := func() {
		Attribute("id")
		Attribute("from_top")
		Attribute("from_bottom")
		Attribute("score")
		Attribute("payload")
	}
	def := func() {
		tiny()
		Attribute("list_id")
		Attribute("updated_at")
		Attribute("tie_breaker")
	}
	full := func() {
		def()
		Attribute("neighbors")
		Attribute("local_from_top")
		Attribute("local_from_bottom")
	}

	View("tiny", tiny)
	View("default", def)
	View("full", full)
	View("full-update", func() {
		full()
		Attribute("previous_rank")
	})
})

var ElementRanks = MediaType("application/vnd.rankdb.multielement+json", func() {
	Attributes(func() {
		Attribute("found", CollectionOf("application/vnd.rankdb.element+json"), func() {
			Description("Elements that was found in the list, ordered by score.")
		})
		Attribute("not_found", ArrayOf(Integer), func() {
			Description("Elements that wasn't found in the list. Unordered.")
			Metadata("struct:field:type", "[]uint64")
		})
	})
	View("default", func() {
		Attribute("found")
		Attribute("not_found")
	})
	Required("found")
})
