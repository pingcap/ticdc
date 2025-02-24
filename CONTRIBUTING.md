# How to contribute

This document outlines some of the conventions on development workflow, commit
message formatting, contact points and other resources to make it easier to get
your contribution accepted.

## Get started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, submit patches!

## Build TiCDC

Developing TiCDC requires:

* [Go 1.23+](https://go.dev/doc/code)
* An internet connection to download the dependencies

Simply run `make cdc` to build the program.

```sh
make cdc
```

### Run tests

TODO

### Update dependencies

TiCDC uses [Go Modules](https://github.com/golang/go/wiki/Modules) to manage dependencies. To add or update a dependency: use the `go mod edit` command to change the dependency.

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work. This is usually `master`.
- Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
- Run tests and make sure all the tests are passed.
- Make sure your commit messages are in the proper format (see below).
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request.
- Your PR must receive LGTMs from two maintainers.

Thanks for your contributions!

### Code style

The coding style suggested by the Golang community is used in TiCDC. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make TiCDC easy to review, maintain and develop.

### Commit message format

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```shell
maintainer: add comment for variable declaration

Improve documentation.
```

The format can be described more formally as follows:

```shell
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the second line is always blank, and other lines should be wrapped at 80 characters. This allows the message to be easier to read on GitHub as well as in various git tools.

- If the change affects more than one subsystem, use a comma to separate them like ```maintainer,dispatcher:```.
- If the change affects many subsystems, use ```*``` instead, like ```*:```.

For the why part, if no specific reason for the change, you can use one of some generic reasons like "Improve documentation.", "Improve performance.", "Improve robustness.", "Improve test coverage."
