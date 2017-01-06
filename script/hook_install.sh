#! /bin/sh

touch .git/hooks/pre-commit || exit
ln -s -f ../../script/pre-commit .git/hooks/pre-commit 
