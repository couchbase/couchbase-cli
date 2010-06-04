/*
  membase_cli.cc

  This program is the top level source file for the Membase Command Line Tools

  ... more docs to come ...
*/

#include <iostream>
#include <string>
#include <algorithm>
#include <tclap/CmdLine.h>

using namespace std;

class MembaseCli {
public:
  virtual MembaseCli () {}
  virtual string runCmd () {}
  virtual ~MembaseCli () {}
}


