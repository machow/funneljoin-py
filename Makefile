README.md: README.Rmd
	jupytext --from Rmd --to ipynb --output - $^ | \
		jupyter nbconvert --stdin --to markdown \
		--TagRemovePreprocessor.remove_input_tags="{'hide-input'}" \
	    --execute --output $@


