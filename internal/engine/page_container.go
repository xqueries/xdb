package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine/page"
)

// PageContainer is a re-usable way to load and unload pages from
// a cache. This is useful when the same pages are loaded and unloaded
// a lot of times.
//
//	myPageContainer := engine.NewPageContainer(myPageID)
//	myPage, _ := myPageContainer.Load()
//	myPage.SomeOperation()
//	myPageContainer.Unload()
//
// This type can be used to "store" a page in a struct without
// having the page pinned in the cache for the time that the
// struct is alive.
//
//	type MyImportantStruct struct {
//		...
//		ImportantPage PageContainer
//		...
//	}
//
// In the above example, you can load and unload your important page
// many times without having to access the engine's page cache every time.
//
// BE CAREFUL TO NOT USE THE PAGE ANY MORE AFTER YOU UNLOAD IT! THIS CONTAINER
// DOES NOT ZERO THE POINTER AFTER UNLOADING, SINCE THAT IS LIKELY TO CAUSE
// A RACE CONDITION.
type PageContainer struct {
	pageID  page.ID
	load    func() (*page.Page, error)
	release func()
}

// NewPageContainer creates a new PageContainer for a page with the given page ID.
func (e Engine) NewPageContainer(id page.ID, load func() (*page.Page, error), release func()) PageContainer {
	return PageContainer{
		pageID:  id,
		load:    load,
		release: release,
	}
}

// Load loads the page of this page container from the cache. It also pins
// the page in the cache.
// To unpin the page in the cache, call Unload.
func (c PageContainer) Load() (*page.Page, error) {
	p, err := c.load()
	if err != nil {
		return nil, fmt.Errorf("fetch and pin: %w", err)
	}
	return p, nil
}

// Unload unpins the page of this container in the cache.
// This does not cause the page to be synchronized with secondary storage.
func (c PageContainer) Unload() {
	c.release()
}
