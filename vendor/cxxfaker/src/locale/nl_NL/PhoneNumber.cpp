/*  tripping-cyril
 *  Copyright (C) 2014  Toon Schoenmakers
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "locale/nl_NL/PhoneNumber.h"

#include <iostream>

namespace cxxfaker {
  namespace providers {
    namespace nl_NL {

PhoneNumber::PhoneNumber(randGenerator* generator, randSeeder* seeder)
: cxxfaker::providers::PhoneNumber(generator, seeder) {
  formats.clear(); // We don't want the default one..
  formats.push_back("+31(0)#########");
  formats.push_back("+31(0)### ######");
  formats.push_back("+31(0)## #######");
  formats.push_back("+31(0)6 ########");
  formats.push_back("+31#########");
  formats.push_back("+31### ######");
  formats.push_back("+31## #######");
  formats.push_back("+316 ########");
  formats.push_back("0#########");
  formats.push_back("0### ######");
  formats.push_back("0## #######");
  formats.push_back("06 ########");
  formats.push_back("(0###) ######");
  formats.push_back("(0##) #######");
  formats.push_back("+31(0)###-######");
  formats.push_back("+31(0)##-#######");
  formats.push_back("+31(0)6-########");
  formats.push_back("+31###-######");
  formats.push_back("+31##-#######");
  formats.push_back("+316-########");
  formats.push_back("0###-######");
  formats.push_back("0##-#######");
  formats.push_back("06-########");
  formats.push_back("(0###)-######");
  formats.push_back("(0##)-#######");
};

    };
  };
};