include ../../Common.mk

.PHONY: gen
gen:
	$(TL_GEN) --language=go --outdir=./checkpoint/gen2/ -v \
		--pkgPath=gitlab.mvk.com/go/vkgo/pkg/sqlitev2/checkpoint/gen2/tl \
		--basicPkgPath=gitlab.mvk.com/go/vkgo/pkg/basictl \
		--generateByteVersions=sqlite. \
		--generateLegacyJsonRead=false \
		./checkpoint/metainfo.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./checkpoint/gen2/...
