#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>

#include "date.h"

namespace {
const int kDepShift = 100;
}

using namespace date;

using IdT = unsigned int;
using SparseMatrix = std::unordered_map<IdT, std::unordered_map<IdT, double>>;

struct User {
  IdT id;
  std::vector<IdT> tracks{};
};

size_t CalcSize(const SparseMatrix &matrix) {
  size_t deps_c = 0;
  for (const auto &it : matrix) {
    deps_c += it.second.size();
  }
  return deps_c;
}

User ParseUser(const std::string &line) {
  static const int kIdIdx = 11;
  const auto id_e_p = line.find("u;");
  User res{static_cast<IdT>(std::stoi(line.substr(kIdIdx, id_e_p - kIdIdx)))};
  auto tr_s_p = id_e_p + 15;
  auto tr_e_p = line.find(";", tr_s_p);
  auto tracks_e_p = line.rfind("]");
  while (tr_e_p < tracks_e_p) {
    res.tracks.push_back(
        static_cast<IdT>(std::stoi(line.substr(tr_s_p, tr_e_p - tr_s_p))));
    tr_s_p = tr_e_p + 1;
    tr_e_p = line.find(";", tr_s_p);
  }

  return res;
}

int main() {
  std::cout << "started_at " << std::chrono::system_clock::now() << std::endl;
  std::ifstream is("data_train_1kk.yson");
  std::string line;
  SparseMatrix tracks_deps;
  // std::unordered_map<int, int> popularity;
  while (std::getline(is, line)) {
    auto user = ParseUser(line);
    for (int i = 0; i < user.tracks.size(); i++) {
      auto upper_bound =
          std::min(static_cast<int>(user.tracks.size()), i + kDepShift);
      for (int j = i; j < upper_bound; j++) {
        tracks_deps[user.tracks[i]][user.tracks[j]] += kDepShift - (j - i);
      }
      // popularity[user.tracks[i]] += 1;
    }
    if (user.id % 10000 == 0) {
      std::cout << "User " << user.id << "; deps: " << CalcSize(tracks_deps)
                << "; " << std::chrono::system_clock::now() << std::endl;
    }
  }
  std::cout << "finished at " << std::chrono::system_clock::now() << std::endl;
  std::cout << "deps_c = " << CalcSize(tracks_deps) << std::endl;
  std::cin >> line;
  return 0;
}
