.PHONY: install-cli uninstall-cli test-cli

CLI_INSTALL_DIRECTORY ?= $(HOME)/.local/bin
CLI_INSTALL_PATH := $(CLI_INSTALL_DIRECTORY)/kubesizer

install-cli:
	install -d "$(CLI_INSTALL_DIRECTORY)"
	@if [ -e "$(CLI_INSTALL_PATH)" ] || [ -L "$(CLI_INSTALL_PATH)" ]; then \
		if [ -L "$(CLI_INSTALL_PATH)" ] && [ "$$(readlink "$(CLI_INSTALL_PATH)")" = "$(CURDIR)/scripts/kubesizer" ]; then \
			echo "Already installed $(CLI_INSTALL_PATH)"; \
		else \
			echo "Refusing to replace existing $(CLI_INSTALL_PATH)" >&2; \
			exit 1; \
		fi; \
	else \
		ln -s "$(CURDIR)/scripts/kubesizer" "$(CLI_INSTALL_PATH)"; \
		echo "Installed $(CLI_INSTALL_PATH)"; \
	fi

uninstall-cli:
	@if [ -L "$(CLI_INSTALL_PATH)" ] && [ "$$(readlink "$(CLI_INSTALL_PATH)")" = "$(CURDIR)/scripts/kubesizer" ]; then \
		rm "$(CLI_INSTALL_PATH)"; \
		echo "Removed $(CLI_INSTALL_PATH)"; \
	else \
		echo "Did not remove $(CLI_INSTALL_PATH): it is not this repository's launcher"; \
	fi

test-cli:
	python3 -m pytest -q tests/test_cli.py
