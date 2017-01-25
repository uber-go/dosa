# Developing dosa

This doc is intended for contributors to uber-go/dosa

## Development Environment

* Go. Install on OS X with `brew install go`. Make sure `go version` returns at
  least `1.7` since we're going to be using 1.7+ features like subtests.

## Checking out the code

Make sure the repository is cloned to the correct location:

```bash
go get github.com/uber-go/dosa/...
cd $GOPATH/src/github.com/uber-go/dosa
```

## Dependency management

Dependencies are tracked via `glide.yaml`. If you're not familiar with `glide`,
read the [docs](https://github.com/Masterminds/glide#usage).

## Licence headers

This project is Open Source Software, and requires a header at the beginning of
all source files. This is enforced by commit hooks and TravisCI.

To add licence headers, use
[uber-licence](https://github.com/uber/uber-licence):

```lang=bash
make add-uber-licence
```

## Workflow

 * If you have write access to the repo, create a branch, otherwise create a fork
 * create a PR, pick one or more reviewers
 * wait for at least one reviewer to mark it as approved, fix what they ask for
 * once a reviewer has approved it, the original author then merges it if they can;
   if there are conflicts, these are resolved by the original author
 * Minor fixes by a project maintainer do not require a re-review, unless you want one

## Commit Messages

At Uber, we follow the [Chris Beams](http://chris.beams.io/posts/git-commit/) guide to
writing git commit messages. Read it, follow it, learn it, love it.

## FIXMEs

If you ever are in the middle of writing code and remember a thing you need to
do, leave a comment like:

```go
// FIXME(rk) make this actually work
```

Your initials in the parens are optional but a good practice. This is better
than a TODO because our CI checks for unresolved FIXMEs, so if you forget to fix
it, your code won't get merged.

## Testing

Run all the tests with coverage and race detector enabled:

```bash
make test RACE=-race
```

### Viewing HTML coverage

```bash
make coverage.html && open coverage.html
```

You'll need to have [gocov-html](https://github.com/matm/gocov-html) installed:

```bash
go get -u gopkg.in/matm/v1/gocov-html
```

## Package Documentation

Dosa uses [md-to-godoc](https://github.com/sectioneight/md-to-godoc) to
generate `doc.go` package documentation from `README.md` markdown syntax. This
means that all package-level documentation is viewable both on GitHub and
[godoc.org](https://godoc.org/github.com/uber-go/dosa).

To document a new package, simply create a `README.md` in the package directory.
Once you're satisfied with its contents, run `make gendoc` from the root of the
project to re-build the `doc.go` files.

Note that changes to documentation may take a while to propagate to godoc.org.
If you check in a change to package documentation, you can manually trigger a
refresh by scrolling to the bottom of the page on godoc.org and clicking
"Refresh Now".
