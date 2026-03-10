include ../../Common.mk

.PHONY: gen
gen:
	$(TL_GEN) --language=go --outdir=./checkpoint/gen2/ -v \
		--pkgPath=gitlab.corp.mail.ru/vkgo/vkgo/pkg/sqlitev2/checkpoint/gen2/tl \
		--basicPkgPath=gitlab.corp.mail.ru/vkgo/vkgo/pkg/basictl \
		--generateByteVersions=sqlite. \
		--generateLegacyJsonRead=false \
		./checkpoint/metainfo.tl
	@echo "Checking that generated code actually compiles..."
	@go build ./checkpoint/gen2/...
