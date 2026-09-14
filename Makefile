# mautrix-whatsapp, Mestra fork. `make help` lists the targets.
.PHONY: help build release

help:
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build ./mautrix-whatsapp
	./build.sh

release: ## Tag a release and push it, which builds and deploys the image
	t=release-$$(date +%Y-%m-%d-%H%M%S) && git tag $$t && git push origin $$t
