#!/usr/bin/python
import sys

date = "&date; "

# searches string (starting from "startIndex") and returns index after next occurrence of "transition" or cancelString
# returns -1 if end of string is found before transition or cancelString
def goToNext(string, transition, startIndex, cancelString):
    substr = ""
    cancelled = False
    while not substr.endswith(transition):
        try:
            substr += string[startIndex]
        except IndexError:
            startIndex = -1
            break           
        startIndex += 1
        if substr.endswith(cancelString):
            cancelled = True
            break
    return startIndex, cancelled

# searches baseString (starting from "startIndex") and if one of the Strings in searchStrings is found, returns index after said String and the String itself
# returns -1 if end of baseString is found before transition or cancelString
def goToNextMultCancel(baseString, startIndex, searchStrings):
    index = startIndex
    substr = ""
    resultString = ""
    fatalError = False
    resultFound = False
    while not fatalError and not resultFound:
        try:
            substr += baseString[index]
        except IndexError:
            index = -1
            fatalError = True
            break

        for item in searchStrings:
            if substr.endswith(item):
                resultString = item
                resultFound = True
                break

        index += 1
    return index, resultString

if len(sys.argv) != 2:
    print "File location not given (or too many arguments)"

# Read list of files to search in and save them in a list
files = []
with open (sys.argv[1], 'rt') as filesource:
    files = filesource.read().splitlines()

for tfile in files:
    # Open file (from argument) and close after operations
    with open (tfile, 'rt') as myfile:
        contents = myfile.read()

    count = 0
    countError = 0
    fatalError = False
    index = 0
    substr = ""
    transitionTable = "<Parameter name=\"table\">"       # substring to search for .. <Parameter name="table">
    transitionSelect = "select "
    transitionWhere = " where "
    transitionOr = " or "
    transitionParameter = "</Parameter>"

    while index < len(contents):                        # While index has not exceeded string length,
        endOfStatement = False
        index = contents.find(transitionTable, index)   # set index to first occurrence of substr
        if index == -1:                                 # If nothing was found,
            break                                       # exit the while loop.
        
        # STATE: 0

        firstLoop = True

        index, cancelled = goToNext(contents, transitionSelect, index, transitionParameter)
        # <Parameter> was never closed, file corrupted anyway
        if index == -1:
            fatalError = True
            break
        # No transitionSelect found
        if cancelled:
            if firstLoop:
                countError += 1
            continue
        # STATE: 1

        while not fatalError and not endOfStatement:
            index, cancelled = goToNext(contents, transitionWhere, index, transitionParameter)
            # <Parameter> was never closed, file corrupted anyway
            if index == -1:
                fatalError = True
                break
            # No transitionWhere found --- should be checked before changing index if where should be inserted
            if cancelled:
                if firstLoop:
                    countError += 1
                break
            # STATE: 2
            contents = contents[:index] + date + contents[index:]
            count += 1

            while not fatalError:
                index, resultString = goToNextMultCancel(contents, index, [transitionOr, transitionSelect, transitionParameter])
                # <Parameter> was never closed, file corrupted anyway
                if index == -1:
                    fatalError = True
                    break
                # switch
                if resultString == transitionOr:
                    # STATE: 2
                    contents = contents[:index] + date + contents[index:]
                    count += 1
                elif resultString == transitionSelect:
                    # STATE: 1
                    break
                elif resultString == transitionParameter:
                    endOfStatement = True
                    # STATE: 3
                    break

            firstLoop = False
            
        if fatalError:
            print tfile + " is corrupted --> <Parameter>-tag never closed, continuing with next file."
            break

    with open (tfile, 'wt') as myfile:
        myfile.write(contents)

    print tfile + ":\t" + str(count) + " dates added"
    # print how many times transitionTable was found but no changes were made
    print str(countError) + " <Parameter>-tags found with no changes made."
    print "------------------------------------------------------------"

# TODO: join statements fixen
# TODO: wenn date vorhanden, dann nicht mehr hinzufuegen?
# TODO: Testing
