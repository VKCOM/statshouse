// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitev2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	itemsOwnerID = iota
	itemsAlbumID
	itemsID
	itemsPreview
	itemsLatitude
	itemsClickableStickers
)

func Test_Entity(t *testing.T) {
	t.Run("CreateTableQuery", func(t *testing.T) {
		q := `CREATE TABLE IF NOT EXISTS items (__delete INT,__order INT NOT NULL,owner_id INT NOT NULL,album_id INT NOT NULL,id INT NOT NULL,preview BLOB DEFAULT a,latitude REAL,clickable_stickers BLOB, PRIMARY KEY (owner_id,album_id,id), UNIQUE (id), UNIQUE (preview,id)) WITHOUT ROWID;`

		items := NewEntity("items")

		items.SetPrimaryColumn("owner_id", NewIntColumn(), itemsOwnerID)
		items.SetPrimaryColumn("album_id", NewIntColumn(), itemsAlbumID)
		items.SetPrimaryColumn("id", NewIntColumn(), itemsID)

		items.PrimaryKey(itemsOwnerID, itemsAlbumID, itemsID)

		items.SetColumn("preview", NewBlobColumn(), itemsPreview).Default(NewDfltValue("a"))
		items.SetColumn("latitude", NewRealColumn(), itemsLatitude)
		items.SetConstColumn("clickable_stickers", NewBlobColumn(), itemsClickableStickers)
		items.SetTechColumn("__order", NewIntColumn(), -1).NotNull()
		items.SetTechColumn("__delete", NewIntColumn(), -2)

		items.UniqueConstraint(itemsID)
		items.UniqueConstraint(itemsPreview, itemsID)

		require.Equal(t, q, items.createTableQuery())
	})
}
