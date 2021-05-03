#include <algorithm>
#include <chrono>
#include <fstream>
#include <future>
#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>

#include "date.h"

namespace {
const int kDepShift = 100;
const int kThreads = 4;
const int kDumpEvery = 100000;
} // namespace

using namespace date;

using IdT = unsigned int;
using SparseMatrix = std::unordered_map<IdT, std::unordered_map<IdT, double>>;

struct Data {
  SparseMatrix deps;
  std::unordered_map<int, int> popularity;
};

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

/**
 * Data format:
 * <tracks_cnt>
 * <track_id> <deps_cnt> <popularity>
 * <depended_track_id> <weight>
 * ...
 * <depended_track_id> <weight>
 * ...
 */
void Save(const Data &data, const std::string &filename) {
  static const char kSep = ' ';
  std::ofstream os(filename);
  os << data.deps.size() << std::endl;
  for (const auto it : data.deps) {
    os << it.first << kSep << it.second.size() << kSep
       << data.popularity.at(it.first) << std::endl;
    for (const auto jt : it.second) {
      os << jt.first << kSep << jt.second << std::endl;
    }
  }
  os.close();
}

/**
 * Data format: see Save()
 */
Data Load(const std::string &filename) {
  std::ifstream is(filename);
  Data res;
  int tracks_cnt;
  is >> tracks_cnt;
  for (int i = 0; i < tracks_cnt; i++) {
    int id, deps_cnt, popularity;
    is >> id >> deps_cnt >> popularity;
    res.popularity[id] = popularity;
    auto &track_deps = res.deps[id];
    for (int j = 0; j < deps_cnt; j++) {
      int dep_tr_id, weight;
      is >> dep_tr_id >> weight;
      track_deps[dep_tr_id] = weight;
    }
  }
  return res;
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

template <typename TIt>
void ConstructAndSaveData(TIt begin, TIt end, int batch_id) {
  Data tracks_deps;
  int cnt = 0;
  int dump_id = 0;
  for (auto it = begin; it < end; it++) {
    const User &user = *it;
    for (int i = 0; i < user.tracks.size(); i++) {
      auto upper_bound =
          std::min(static_cast<int>(user.tracks.size()), i + kDepShift);
      for (int j = i; j < upper_bound; j++) {
        tracks_deps.deps[user.tracks[i]][user.tracks[j]] += kDepShift - (j - i);
      }
      tracks_deps.popularity[user.tracks[i]] += 1;
    }
    if (++cnt >= kDumpEvery) {
      auto filename =
          "data_" + std::to_string(batch_id) + "_" + std::to_string(dump_id);
      std::cout << "Start dump " << filename << "; "
                << std::chrono::system_clock::now() << std::endl;
      Save(tracks_deps, filename);
      tracks_deps.deps.clear();
      tracks_deps.popularity.clear();
      dump_id++;
    }
  }
  if (!tracks_deps.deps.empty())
    Save(tracks_deps,
         "data_" + std::to_string(batch_id) + "_" + std::to_string(dump_id));
}

int main() {
  std::cout << "started_at " << std::chrono::system_clock::now() << std::endl;
  std::ifstream is("data_train_100.yson");
  std::string line;
  std::vector<User> users;
  users.reserve(100);
  while (std::getline(is, line)) {
    users.push_back(ParseUser(line));
  }
  std::cout << "finish reading at " << std::chrono::system_clock::now()
            << std::endl;
  std::vector<std::future<void>> tasks;
  tasks.reserve(kThreads);
  auto shift = users.size() / kThreads; // suppose, it divided
  auto it_b = users.begin();
  auto it_e = users.begin() + shift;
  int thread_num = 0;
  while (it_b < users.end()) {
    tasks.push_back(std::async(std::launch::async,
                               ConstructAndSaveData<decltype(it_b)>, it_b, it_e,
                               thread_num));
    it_b = it_e;
    it_e += shift;
    thread_num++;
  }
  std::cout << "all threads started at " << std::chrono::system_clock::now()
            << std::endl;
  for (auto &fut : tasks) {
    fut.get();
  }
  std::cout << "finished at " << std::chrono::system_clock::now() << std::endl;
  return 0;
}
