/******************************************************************************
 * Copyright 2015 Matthieu Lefebvre
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <boost/mpi/collectives.hpp>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <boost/format.hpp>
#include <boost/filesystem.hpp>

#include <string>
#include <iostream>
#include <fstream>
#include <regex>
#include <exception>

namespace mpi = boost::mpi;
namespace ptree = boost::property_tree;


/** 
 * @brief Retrieves values from Par_file.json
 * 
 * @param env
 * @param nproc
 * @param nsimultaneous
 */
void parse_params(mpi::environment &env, int &nproc, int &nsimultaneous) {
  try {
  ptree::ptree pt;
  ptree::read_json("DATA/Par_file.json", pt);
  nproc = pt.get<int>("nproc");
  nsimultaneous = pt.get<int>("number_simultaneous_runs");
  } catch (std::exception &e) {
    std::cerr << "Check DATA/Par_file.json" << std::endl;
    env.abort(-1);
  }
}

int get_int_value_from_regex(std::ifstream &ifs, const std::regex &pattern) {
  for (std::string line; getline(ifs,line);) {
    std::cout << line << std::endl;
    std::smatch matches;

    if (std::regex_search(line ,matches,pattern)) // search for pat in line
      std::cout << ": " << matches[1] << '\n';
      return std::stoi(matches[1]);
  }
  throw (std::runtime_error("Value not found"));
}

void parse_params_text(mpi::environment &env, int &nproc, int &nsimultaneous) {
  try {
    std::ifstream ifs("DATA/Par_file");
    nproc = get_int_value_from_regex(ifs,
        std::regex(R"(NPROC\s*=\s*(\d+))"));
    nsimultaneous = get_int_value_from_regex(ifs, 
        std::regex(R"(NUMBER_OF_SIMULTANEOUS_RUNS\s*=\s*(\d+))"));
  } catch (std::exception &e) {
    std::cerr << "Check DATA/Par_file" <<std::endl;
    env.abort(-1);
  }
}

/** 
 * @brief Aborts program if directory does not exist.
 * 
 * @param env
 * @param dir_path
 */
void check_dir_exists(mpi::environment &env, std::string &dir_path) {
    if (!boost::filesystem::is_directory(dir_path)) {
      std::cerr << dir_path << " should exists" << std::endl;
      env.abort(-1);
    }
}

/** 
 * @brief Writes of file with the last lines of a regular 
 *        specfem3d_globe output
 * 
 * @param dir
 */
void write_mocked_output_solver(std::string &dir) {
  // To be able to check if everything has been written correctly
  // A little bit more than needed, just to be sure.
  std::string output_solver_txt = 
      " Time-Loop Complete. Timing info:\n"
      " Total elapsed time in seconds =    454.138031005859\n"
      " Total elapsed time in hh:mm:ss =      0 h 07 m 34 s\n"
      "\n"
      " End of the simulation\n"
      "\n";

  std::string output_name = str(boost::format("output_solver.txt"));

  std::ofstream output_file;
  output_file.open(dir + "/" + output_name);
  output_file << output_solver_txt;
  output_file.close();
}


int main(int argc, char *argv[]) {
  mpi::environment env;
  mpi::communicator world;

  int nproc, nsimultaneous;

  // Get parameters shared across all events on the master process
  if (0 == world.rank()){
    parse_params_text(env, nproc, nsimultaneous);
    //parse_params(env, nproc, nsimultaneous);
    std::cout << "Reading nproc from par file: " << nproc << std::endl;
    std::cout << "Reading nsimultaneous from par file: " << nsimultaneous << std::endl;

    if (world.size() % (nproc * nsimultaneous)) {
      std::cerr << "num mpi processes should be nproc * nsimultaneous" 
                << std::endl;
      env.abort(-1);
    }
  }

  // Broadcast shared parameters to the world
  broadcast(world, nproc, 0);
  broadcast(world, nsimultaneous, 0);

  // Create subcommunicators, only if there is several simultaneous events
  int color;
  mpi::communicator local;
  if (nsimultaneous > 1) {
    color = world.rank() / nsimultaneous;
    local = world.split(color);
  } else {
    color = 0;
    local = world;
  }

  // Every event has its own work directory. Index starts at 1.
  std::string local_workdir, local_output_dir, local_data_dir;

  local_workdir = str(boost::format("./run%04d/") % (color+1)); 
  local_output_dir = local_workdir + "/OUTPUT_FILES/";
  local_data_dir = local_workdir + "/DATA/";

  // Only local masters need to output fake data
  if (0 == local.rank()) {
    check_dir_exists(env, local_workdir);
    check_dir_exists(env, local_output_dir);
    check_dir_exists(env, local_data_dir);

    write_mocked_output_solver(local_output_dir);

    std::string stations_path = local_data_dir + "/STATIONS";
    std::ifstream stations_stream(stations_path);

    std::string station, network;
    float latitude, longitude, depth, elevation;

    while (stations_stream >> station >> network 
                           >> latitude >> longitude >> depth >> elevation) {
      std::cout << station << " -- " << network << std::endl;
      std::string channels[] = {"MXE", "MXN", "MXZ"};
      for (auto channel : channels) {
        std::string sac_name = network + "." + station + "."
                             + channel + ".sac";
        std::ofstream sac_file (local_output_dir + sac_name);
        sac_file.close();
      }
    }
    stations_stream.close();
  }

  //std::string output_name;
  //output_name = str(boost::format("output_local%04d") % local.rank()); 
  //std::ofstream output_file;
  //output_file.open(local_workdir + "/" + output_name);
  //output_file << "hello\n";
  //output_file.close();

  return 0;
}
