#!/usr/bin/env python
# encoding: utf-8

"""A tool to change or add license headers in all supported files in or below a directory."""

from __future__ import unicode_literals
from __future__ import print_function


import os
import shutil
import sys
import logging
import argparse
import re
import fnmatch
from string import Template
from shutil import copyfile
import io
import subprocess
import os.path


__version__ = '0.1'


log = logging.getLogger(__name__)


try:
    unicode
except NameError:
    unicode = str



## for each processing type, the detailed settings of how to process files of that type
typeSettings = {
    "go": {
        "extensions": [".go"],
        "keepFirst": None,
        "blockCommentStartPattern": None,  ## used to find the beginning of a header bloc
        "blockCommentEndPattern": None,   ## used to find the end of a header block
        "lineCommentStartPattern": re.compile(r'\s*//'),    ## used to find header blocks made by line comments
        "lineCommentEndPattern": None,
        "headerStartLine": "",   ## inserted before the first header text line
        "headerEndLine": "",    ## inserted after the last header text line
        "headerLinePrefix": "// ",   ## inserted before each header text line
        "headerLineSuffix": None,            ## inserted after each header text line, but before the new line
    },
    "java": {
        "extensions": [".java",".scala",".groovy",".jape"],
        "keepFirst": None,
        "blockCommentStartPattern": re.compile('^\s*/\*'),  ## used to find the beginning of a header bloc
        "blockCommentEndPattern": re.compile(r'\*/\s*$'),   ## used to find the end of a header block
        "lineCommentStartPattern": re.compile(r'\s*//'),    ## used to find header blocks made by line comments
        "lineCommentEndPattern": None,
        "headerStartLine": "/*\n",   ## inserted before the first header text line
        "headerEndLine": " */\n",    ## inserted after the last header text line
        "headerLinePrefix": " * ",   ## inserted before each header text line
        "headerLineSuffix": None,            ## inserted after each header text line, but before the new line
    },
}

yearsPattern = re.compile("Copyright\s*(?:\(\s*[C|c|©]\s*\)\s*)?([0-9][0-9][0-9][0-9](?:-[0-9][0-9]?[0-9]?[0-9]?))",re.IGNORECASE)
licensePattern = re.compile("license",re.IGNORECASE)
emptyPattern = re.compile(r'^\s*$')

## -----------------------

## maps each extension to its processing type. Filled from tpeSettings during initialization
ext2type = {}
patterns = []

def parse_command_line(argv):
    """Parse command line argument. See -h option.

    Arguments:
      argv: arguments on the command line must include caller file name.

    """
    import textwrap

    example = textwrap.dedent("""
      ## Some examples of how to use this command!
    """).format(os.path.basename(argv[0]))
    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description="Python license header updater",
                                     epilog=example,
                                     formatter_class=formatter_class)
    parser.add_argument("-V", "--version", action="version",
                        version="%(prog)s {}".format(__version__))
    parser.add_argument("-v", "--verbose", dest="verbose_count",
                        action="count", default=0,
                        help="increases log verbosity (can be specified "
                        "multiple times)")
    parser.add_argument("-d", "--dir", dest="dir", nargs=1,
                        help="The directory to recursively process.")
    parser.add_argument("-t", "--tmpl", dest="tmpl", nargs=1,
                        help="Template name or file to use.")
    parser.add_argument("-y", "--years", dest="years", nargs=1,
                        help="Year or year range to use.")
    parser.add_argument("-o", "--owner", dest="owner", nargs=1,
                        help="Name of copyright owner to use.")
    parser.add_argument("-n", "--projname", dest="projectname", nargs=1,
                        help="Name of project to use.")
    parser.add_argument("-u", "--projurl", dest="projecturl", nargs=1,
                        help="Url of project to use.")
    arguments = parser.parse_args(argv[1:])

    # Sets log level to WARN going more verbose for each new -V.
    log.setLevel(max(3 - arguments.verbose_count, 0) * 10)
    return arguments

exclude = set(["vendor", ".git"])
def get_paths(patterns, start_dir="."):
    """Retrieve files that match any of the glob patterns from the start_dir and below."""
    for root, dirs, files in os.walk(start_dir):
        names = []
        dirs[:] = [d for d in dirs if d not in exclude]
        for pattern in patterns:
            names += fnmatch.filter(files, pattern)
        for name in names:
            path = os.path.join(root, name)
            yield path

## return an array of lines, with all the variables replaced
## throws an error if a variable cannot be replaced
def read_template(templateFile,dict):
    with open(templateFile,'r') as f:
        lines = f.readlines()
    lines = [Template(line).substitute(dict) for line in lines]  ## use safe_substitute if we do not want an error
    return lines

## format the template lines for the given type
def for_type(templatelines,type):
    lines = []
    settings = typeSettings[type]
    headerStartLine = settings["headerStartLine"]
    headerEndLine = settings["headerEndLine"]
    headerLinePrefix = settings["headerLinePrefix"]
    headerLineSuffix = settings["headerLineSuffix"]
    if headerStartLine is not None:
        lines.append(headerStartLine)
    for l in templatelines:
        tmp = l
        if headerLinePrefix is not None:
            if len(tmp.strip()) == 0:
                tmp = headerLinePrefix.rstrip() + tmp
            else:
                tmp = headerLinePrefix + tmp
        if headerLineSuffix is not None:
            tmp = tmp + headerLineSuffix
        lines.append(tmp)
    if headerEndLine is not None:
        lines.append(headerEndLine)
    return lines


## read a file and return a dictionary with the following elements:
## lines: array of lines
## skip: number of lines at the beginning to skip (always keep them when replacing or adding something)
##   can also be seen as the index of the first line not to skip
## headStart: index of first line of detected header, or None if non header detected
## headEnd: index of last line of detected header, or None
## yearsLine: index of line which contains the copyright years, or None
## haveLicense: found a line that matches a pattern that indicates this could be a license header
## settings: the type settings
## If the file is not supported, return None
def read_file(file):
    skip = 0
    headStart = None
    headEnd = None
    yearsLine = None
    haveLicense = False
    extension = os.path.splitext(file)[1]
    logging.debug("File extension is %s",extension)
    ## if we have no entry in the mapping from extensions to processing type, return None
    type = ext2type.get(extension)
    logging.debug("Type for this file is %s",type)
    if not type:
        return (None, None)
    settings = typeSettings.get(type)
    with open(file,'r') as f:
        lines = f.readlines()
    ## now iterate throw the lines and try to determine the various indies
    ## first try to find the start of the header: skip over shebang or empty lines
    keepFirst = settings.get("keepFirst")
    blockCommentStartPattern = settings.get("blockCommentStartPattern")
    blockCommentEndPattern = settings.get("blockCommentEndPattern")
    lineCommentStartPattern = settings.get("lineCommentStartPattern")
    i = 0
    for line in lines:
        if i==0 and keepFirst and keepFirst.findall(line):
            skip = i+1
        elif emptyPattern.findall(line):
            pass
        elif blockCommentStartPattern and blockCommentStartPattern.findall(line):
            headStart = i
            break
        elif blockCommentStartPattern and lineCommentStartPattern.findall(line):
            pass
        elif not blockCommentStartPattern and lineCommentStartPattern.findall(line):
            headStart = i
            break
        else:
            ## we have reached something else, so no header in this file
            #logging.debug("Did not find the start giving up at lien %s, line is >%s<",i,line)
            return (type, {"lines":lines, "skip":skip, "headStart":None, "headEnd":None, "yearsLine": None, "settings":settings, "haveLicense": haveLicense})
        i = i+1
    #logging.debug("Found preliminary start at %s",headStart)
    ## now we have either reached the end, or we are at a line where a block start or line comment occurred
    # if we have reached the end, return default dictionary without info
    if i == len(lines):
        #logging.debug("We have reached the end, did not find anything really")
        return (type, {"lines":lines, "skip":skip, "headStart":headStart, "headEnd":headEnd, "yearsLine": yearsLine, "settings":settings, "haveLicense": haveLicense})
    # otherwise process the comment block until it ends
    if blockCommentStartPattern:
        for j in range(i,len(lines)):
            #logging.debug("Checking line %s",j)
            if licensePattern.findall(lines[j]):
                haveLicense = True
            elif blockCommentEndPattern.findall(lines[j]):
                return (type, {"lines":lines, "skip":skip, "headStart":headStart, "headEnd":j, "yearsLine": yearsLine, "settings":settings, "haveLicense": haveLicense})
            elif yearsPattern.findall(lines[j]):
                haveLicense = True
                yearsLine = j
        # if we went through all the lines without finding an end, maybe we have some syntax error or some other
        # unusual situation, so lets return no header
        #logging.debug("Did not find the end of a block comment, returning no header")
        return (type, {"lines":lines, "skip":skip, "headStart":None, "headEnd":None, "yearsLine": None, "settings":settings, "haveLicense": haveLicense})
    else:
        for j in range(i,len(lines)-1):
            if lineCommentStartPattern.findall(lines[j]) and licensePattern.findall(lines[j]):
                haveLicense = True
            elif not lineCommentStartPattern.findall(lines[j]):
                return (type, {"lines":lines, "skip":skip, "headStart":i, "headEnd":j-1, "yearsLine": yearsLine, "settings":settings, "haveLicense": haveLicense})
            elif yearsPattern.findall(lines[j]):
                haveLicense = True
                yearsLine = j
        ## if we went through all the lines without finding the end of the block, it could be that the whole
        ## file only consisted of the header, so lets return the last line index
        return (type, {"lines":lines, "skip":skip, "headStart":i, "headEnd":len(lines)-1, "yearsLine": yearsLine, "settings":settings, "haveLicense": haveLicense})

def make_backup(file):
    copyfile(file,file+".bak")

def main():
    """Main function."""
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    ## init: create the ext2type mappings
    for type in typeSettings:
        settings = typeSettings[type]
        exts = settings["extensions"]
        for ext in exts:
            ext2type[ext] = type
            patterns.append("*"+ext)

    try:
        error = False
        settings = {
        }
        templateLines = None
        arguments = parse_command_line(sys.argv)
        if arguments.dir:
            start_dir = arguments.dir[0]
        else:
            start_dir = "."
        if arguments.years:
            settings["years"] = arguments.years[0]
        if arguments.owner:
            settings["owner"] = arguments.owner[0]
        if arguments.projectname:
            settings["projectname"] = arguments.projectname[0]
        if arguments.projecturl:
            settings["projecturl"] = arguments.projecturl[0]
        ## if we have a template name specified, try to get or load the template
        if arguments.tmpl:
            opt_tmpl = arguments.tmpl[0]
            ## first get all the names of our own templates
            ## for this get first the path of this file
            templatesDir = os.path.join(os.path.dirname(os.path.abspath(__file__)),"templates")
            ## get all the templates in the templates directory
            templates = [f for f in get_paths("*.tmpl",templatesDir)]
            templates = [(os.path.splitext(os.path.basename(t))[0],t) for t in templates]
            ## filter by trying to match the name against what was specified
            tmpls = [t for t in templates if opt_tmpl in t[0]]
            if len(tmpls) == 1:
                tmplName = tmpls[0][0]
                tmplFile = tmpls[0][1]
                templateLines = read_template(tmplFile,settings)
            else:
                if len(tmpls) == 0:
                    ## check if we can interpret the option as file
                    if os.path.isfile(opt_tmpl):
                        templateLines = read_template(os.path.abspath(opt_tmpl),settings)
                    else:
                        error = True
                else:
                    ## notify that there are multiple matching templates
                    error = True
        else: # no tmpl parameter
            if not arguments.years:
                error = True
        if not error:
            #logging.debug("Got template lines: %s",templateLines)
            ## now do the actual processing: if we did not get some error, we have a template loaded or no template at all
            ## if we have no template, then we will have the years.
            ## now process all the files and either replace the years or replace/add the header
            logging.debug("Processing directory %s",start_dir)
            logging.debug("Patterns: %s",patterns)
            for file in get_paths(patterns,start_dir):
                logging.debug("Processing file: %s",file)
                (type, dict) = read_file(file)
                if not dict:
                    logging.debug("File not supported %s",file)
                    continue
                # logging.debug("DICT for the file: %s",dict)
                logging.debug("Info for the file: headStart=%s, headEnd=%s, haveLicense=%s, skip=%s",dict["headStart"],dict["headEnd"],dict["haveLicense"],dict["skip"])
                lines = dict["lines"]
                ## if we have a template: replace or add
                if templateLines:
                    # make_backup(file)
                    with open(file,'w') as fw:
                        ## if we found a header, replace it
                        ## otherwise, add it after the lines to skip
                        headStart = dict["headStart"]
                        headEnd = dict["headEnd"]
                        haveLicense = dict["haveLicense"]
                        skip = dict["skip"]
                        if headStart is not None and headEnd is not None and haveLicense:
                            ## first write the lines before the header
                            fw.writelines(lines[0:headStart])
                            ## now write the new header from the template lines
                            fw.writelines(for_type(templateLines,type))
                            ## now write the rest of the lines
                            fw.writelines(lines[headEnd+1:])
                        else:
                            fw.writelines(lines[0:skip])
                            fw.writelines(for_type(templateLines, type))
                            fw.write("\n")
                            fw.writelines(lines[skip:])
                else: ## no template lines, just update the line with the year, if we found a year
                    yearsLine = dict["yearsLine"]
                    if yearsLine is not None:
                        # make_backup(file)
                        with open(file,'w') as fw:
                            fw.writelines(lines[0:yearsLine])
                            fw.write(yearsPattern.sub(arguments.years,lines[yearsLine]))
    finally:
        logging.shutdown()


if __name__ == "__main__":
    sys.exit(main())
