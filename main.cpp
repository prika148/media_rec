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
const int kThreads = 6;
const int kDumpEvery = 50000;
const int kSaveThreshold = 20;
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
void Save(Data &&data, const std::string &filename) {
  static const char kSep = ' ';
  std::ofstream os(filename);
  os << data.deps.size() << std::endl;
  for (auto it = data.deps.begin(); it != data.deps.end(); it++) {
    if (kSaveThreshold > 0) {
      auto jt = it->second.begin();
      while (jt != it->second.end()) {
        if (jt->second < kSaveThreshold) {
          jt = it->second.erase(jt);
        } else {
          jt++;
        }
      }
    }
    os << it->first << kSep << it->second.size() << kSep
       << data.popularity.at(it->first) << std::endl;
    for (const auto jt : it->second) {
      os << jt.first << kSep << jt.second << std::endl;
    }
    it->second.clear();
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

void ConstructAndSaveData(std::vector<User> &&users, int batch_id) {
  std::cout << "Thread " << batch_id << " spawned at "
            << std::chrono::system_clock::now() << std::endl;
  Data tracks_deps;
  int cnt = 0;
  int dump_id = 0;
  for (auto it = users.begin(); it < users.end(); it++) {
    const User &user = *it;
    for (int i = 0; i < static_cast<int>(user.tracks.size()); i++) {
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
      Save(std::move(tracks_deps), filename);
      std::cout << "finish dump " << filename << "; "
                << std::chrono::system_clock::now() << std::endl;
      tracks_deps.deps.clear();
      tracks_deps.popularity.clear();
      dump_id++;
      cnt = 0;
    }
  }
  if (!tracks_deps.deps.empty())
    Save(std::move(tracks_deps),
         "data_" + std::to_string(batch_id) + "_" + std::to_string(dump_id));
}

int ReadAndSave() {
  std::cout << "started_at " << std::chrono::system_clock::now() << std::endl;
  std::ifstream is("data_train_100.yson");
  std::string line;
  std::vector<User> users;
  std::vector<std::future<void>> tasks;
  auto per_thread = users.size() / kThreads; // suppose, it divided
  std::cout << "Threads: " << kThreads << ", per_thread " << per_thread
            << std::endl;
  int thread_num = 0;
  users.reserve(per_thread);
  tasks.reserve(kThreads);
  while (std::getline(is, line)) {
    users.push_back(ParseUser(line));
    if (users.size() >= per_thread) {
      per_thread = 0;
      tasks.push_back(std::async(std::launch::async, ConstructAndSaveData,
                                 std::move(users), thread_num));
      thread_num++;
      users.clear();
    }
  }
  std::cout << "finish reading at " << std::chrono::system_clock::now()
            << std::endl;
  for (auto &fut : tasks) {
    fut.get();
  }
  std::cout << "finished at " << std::chrono::system_clock::now() << std::endl;
  return 0;
}

int main() { return ReadAndSave(); }
