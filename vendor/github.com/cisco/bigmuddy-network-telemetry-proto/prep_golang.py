#!/usr/bin/env python3

import os
import re
import sys
import errno
import subprocess
import json
import glob


IMPORTPREFIX="github.com/cisco/"
REPOROOT=os.path.basename(os.getcwd())
SRCDIR="staging"
TGTDIR="proto_go"
PACKAGE="^(package .*);"
#
#  Attributes pertinent to building the go code to serve proto types
#  for given gather points
#
#  MAPFILEGLOB matches any platform _yang2proto meta data. Meta data
#  allows us to map the yang gather point to messages in pertinent
#  map files.
MAPFILEGLOB="{}/*_yang2proto_map.json".format(SRCDIR)
GENGOPACKAGE="telemetry"
GENGOMAIN="{}/mdt_telemetry.go".format(TGTDIR)
GENGOMAINSKELETON="etc/mdt_telemetry.go_"
GENGOTEST="{}/mdt_telemetry_test.go".format(TGTDIR)
GENGOTESTSKELETON="etc/mdt_telemetry_test.go_"
GENGODOC="{}/doc.go".format(TGTDIR)
GENGODOCSKELETON="etc/doc.go_"
GENGOREADME="{}/README.md".format(TGTDIR)
GENGOREADMESRC="etc/README_golang.md"
BPXMAP="basepathxlation.json"
GENGOBPXMAP="{}/{}".format(TGTDIR, BPXMAP)

GENGOPREFIX="{}/{}/".format(REPOROOT, TGTDIR)

def walkTree(start):
    for root, dirs, files in os.walk(start):
        yield root, dirs, files

def replace_snake(m):
    if m:
        return m.group(1).upper()
    else:
        return ""

def toCamelCase(snake_str):
    """
    Modelled on protoc-gen-go/generator/generator.go Remember to
    catch the first one too. Logical OR of regexp might have been a
    little neater.

    """
    capital_snake = re.sub("^([a-z])", lambda m: m.group(1).upper(), snake_str)
    camel_onepass = re.sub("_([a-z])", lambda m: m.group(1).upper(), capital_snake)
    camel_twopass = re.sub("([0-9][a-z])", lambda m: m.group(1).upper(), camel_onepass)
    return camel_twopass

def extractSchema(entry):
    if 'schema' in entry:
        return entry['schema']
    else:
        return ""

def createGatherPathMap():
    """
    Create the map with all the content we need to auto generate the
    code for mapping from yang path to corresponding messages.
    """

    # We handle as many instances of mapping files as match the GLOB
    listing = []
    for mapfilename in list(glob.glob(MAPFILEGLOB)):
        with open(mapfilename) as mapfile:
            listing.extend(json.load(mapfile))

    gatherPathMap = []
    for entry in listing:
        # If operator has removed models, simply skip them
        protoFile = "{}/{}".format(SRCDIR, entry['file'])
        if os.path.exists(protoFile):
            gatherPathMap.append({
                'gather_path' : entry['encoding_path'],
                'package' : entry['package'].replace('.','_'),
                'path' : GENGOPREFIX + os.path.dirname(entry['file']),
                'content' : toCamelCase(entry['message']),
                'keys' : toCamelCase(entry['message_keys']),
                'schema': extractSchema(entry)})

    return gatherPathMap

def createDictForPath2Yang(gatherPaths):
    """
    Provide dict to find yang path from proto path
    """
    d = {}
    for g in gatherPaths:
        d[g['path']] = g['gather_path']
    return d

def createImportsString(gatherPaths):
    """
    Build the import statement from the mapping
    """
    # Only the directory is required.
    imports = "\n\t".join(['{} "{}{}"'.format(
        g['package'], IMPORTPREFIX, g["path"]) for g in gatherPaths])
    return ('import ("reflect"\n\t{})'.format(imports))

def createReflectionGoMap(gatherPaths):
    """
    Build the reflection statement for the go code providing the
    pertinent mappings from yang path to message types. Version
    numbers for models will be added once we get this content.
    NOTE: {{ and }} are escaped { and } in format string. You'll
    thank me for writing this down if you are trying to understand
    the go snippet below.
    """
    s = ",\n".join(['ProtoKey{{EncodingPath:"{}",Version:""}}:[]reflect.Type{{reflect.TypeOf((*{}.{})(nil)),reflect.TypeOf((*{}.{})(nil))}}'.format(g["gather_path"], g["package"], g["content"], g["package"], g["keys"]) for g in gatherPaths])

    return ("var schema2message = map[ProtoKey][]reflect.Type{{{}}}".format(s))

def createTestTable(gatherPaths):
    """
    Build the test table from gatherPaths set, and validate generated JSON
    for base path xlation is valid.
    """
    
    testTableHdr = """
var Yang2ProtoTestTable = []Yang2ProtoTestTableEntry{
	{"", "", false, nil, nil},
	{"Cisco-IOS-XR-infra-statsd-oper:infra-statistics", "", false, nil, nil},
	{"Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-NONEXIST", "", false, nil, nil},
	{"Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters/TOOLONG", "", false, nil, nil},
"""
    testTableBdy = ",\n".join(['{{"{}","",true,reflect.TypeOf((*{}.{})(nil)),reflect.TypeOf((*{}.{})(nil))}}'.format(
        g["gather_path"], g["package"], g["keys"], g["package"], g["content"]) for g in gatherPaths])
    return "{}{}}}".format(testTableHdr, testTableBdy)

def createProtolist():
    " get list of protos in [(rootdir, tgtdir, file),...]"
    protolist = []
    for root,dirs,files in walkTree(SRCDIR):
        tgt = root.replace(SRCDIR, TGTDIR, 1)
        for f in files:
            protolist.append((root, tgt, f))

    return protolist

def extractPackageName(filename):
    r = re.compile(PACKAGE)
    with open(filename) as f:
        for line in f:
            m = r.search(line)
            if None != m:
                return m.group(1).replace(".","_")

def extractRelativePath(src, tgt):
    """
    When at src, rooted from same place as tgt, get relative path to
    target. Implementation looks esoteric.

    """
    seps = src.count("/")
    return "{}/{}".format("/".join([".."] * seps), tgt)

def generateGoCode(gatherPathMap):
    """
    Generate go code, tests and docs
    """
    imports = createImportsString(gatherPathMap)
    reflections = createReflectionGoMap(gatherPathMap)
    testtable = createTestTable(gatherPathMap)

    os.makedirs(TGTDIR, exist_ok=True)

    print("Generating {}...".format(GENGOMAIN))
    with open(GENGOMAIN, "w") as t:
        t.write('package {}\n'.format(GENGOPACKAGE))
        t.write(imports + "\n")
        with open(GENGOMAINSKELETON) as skeleton:
            t.write(skeleton.read() + "\n")
        t.write(reflections)

    print("Generating {}...".format(GENGOTEST))
    testBasePathXlationMap = """
func TestBasePathXlationMap(t *testing.T) {{

    bpxJSON, err := ioutil.ReadFile("{}")
    if err != nil {{
        t.Fatal("Failed to open base path xlation map {}:", err)
    }}
    basePathXlation := map[string]string{{}}
    err = json.Unmarshal(bpxJSON, &basePathXlation)
    if err != nil {{
        t.Fatal("Failed to unmarshal to expected structure:", err)
    }}
}}
""".format(BPXMAP, BPXMAP)

    with open(GENGOTEST, "w") as t:
        t.write('package {}\n'.format(GENGOPACKAGE))
        t.write(imports + "\n")
        with open(GENGOTESTSKELETON) as skeleton:
            t.write(skeleton.read() + "\n")
        t.write(testBasePathXlationMap)
        t.write(testtable)

    print("Copying {}...".format(GENGODOC))
    with open(GENGODOC, "w") as t:
        with open(GENGODOCSKELETON) as skeleton:
            t.write(skeleton.read() + "\n")
        # Package at the end for doc.go
        t.write('package {}\n'.format(GENGOPACKAGE))

    print("Copying {}...".format(GENGOREADME))
    with open(GENGOREADME, "w") as readme:
        with open(GENGOREADMESRC) as readmesrc:
            readme.write(readmesrc.read())
            # Straight copy for this one.

def generateBasePathXlationMap():
    print("Generating {}...".format(GENGOBPXMAP))

    # We handle as many instances of mapping files as match the GLOB,
    # much as when we build the gather paths.
    listing = []
    for mapfilename in list(glob.glob(MAPFILEGLOB)):
        with open(mapfilename) as mapfile:
            listing.extend(json.load(mapfile))

    bpx = dict()
    for entry in listing:
        if 'schema' in entry:
            bpx[entry['schema']] =  entry['encoding_path']

    with open(GENGOBPXMAP, 'w') as bpxFile:
        json.dump(bpx, bpxFile, separators=(',\n',': '), sort_keys=True)

if __name__ == "__main__":

    gatherPathMap = createGatherPathMap()
    generateGoCode(gatherPathMap)
    generateBasePathXlationMap()
    path2yang = createDictForPath2Yang(gatherPathMap)
    count = 0
    print("Soft links to protos and adding 'go generate' directive in a.go...")
    l = createProtolist()
    for src,tgt,f in l:

        if not f.endswith(".proto"):
            continue

        count = count + 1

        srcfilename = os.path.join(src, f)
        tgtfilename = os.path.join(tgt, f)
        docsfile = os.path.join(tgt, "a.go")

        package = extractPackageName(srcfilename)

        #
        # Make directory if it does not exist
        os.makedirs(tgt, exist_ok=True)

        #
        # Make symlink
        relativepath = extractRelativePath(tgtfilename, srcfilename)
        try:
            os.symlink(relativepath, tgtfilename)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            pass

        #
        # Write docs for go generate ./...
        # and add package if necessary
        doccontent = """
//go:generate protoc --go_out=plugins=grpc:. {}
        """.format(f)

        if not os.path.exists(docsfile):
            path = "{}/{}".format(REPOROOT, tgt)
            if path in path2yang:
                yangPath = "// " + path2yang[path]
            else:
                yangPath = ""
            doccontent = doccontent + """
{}
{}
            """.format(yangPath, package)

        # A messy way of creating it if it does not exist but reading content
        # looking for previous instances of go gen directives.
        with open(docsfile, "a+") as docfile:
            docfile.seek(0)
            if doccontent not in docfile.read():
                docfile.write(doccontent)


    print("Generating golang bindings for {} .proto files. This stage takes some time...".format(count))
    try:
        subprocess.check_call(["go", "generate", "./..."])
    except subprocess.CalledProcessError as e:
        print("'go generate' interprets .proto and builds go binding")
        print(" *** STAGE DID NOT RUN CLEAN. ERROR MESSAGE ABOVE. COMMON PROBLEMS BELOW ***")
        print(" GOROOT must be set to where golang is installed, minimum version go1.7 to run tests")
        print(" GOPATH must be workspace root")
        print(" Guidelines here: https://golang.org/doc/code.html")
        print(" protoc-gen-go must be in PATH (https://github.com/golang/protobuf)")
        print(" protoc must be in PATH")
        print(" go get -u github.com/golang/protobuf/{proto,protoc-gen-go}")
        print(e)

    print("Building and running tests for package. The build stage also takes some time...")
    try:
        subprocess.check_call(["go", "test", "-run=.", "-bench=.", "{}{}".format(
            IMPORTPREFIX, GENGOPREFIX)])
    except subprocess.CalledProcessError as e:
        print("This stage builds and runs.")
        print(" *** STAGE DID NOT RUN CLEAN. ERROR MESSAGE ABOVE. COMMON PROBLEMS BELOW ***")
        print(" GOROOT must be set to where golang is installed, minimum go1.7 to run subtests")
        print(" GOPATH must be workspace root")
        print(" Guidelines here: https://golang.org/doc/code.html")
        print(e)
        
    print("Done.")
