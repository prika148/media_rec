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
const int kThreads = 8;
const int kCleanEvery = 31250;
const int kSaveThreshold = 20;
} // namespace

using namespace date;

using IdT = unsigned int;
using SparseMatrix = std::unordered_map<IdT, std::unordered_map<IdT, int>>;

struct Data {
  SparseMatrix deps;
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

int Reduce(SparseMatrix &matrix, int threshold) {
  int removed = 0;
  auto it = matrix.begin();
  while (it != matrix.end()) {
    auto jt = it->second.begin();
    while (jt != it->second.end()) {
      if (jt->second < threshold) {
        removed++;
        jt = it->second.erase(jt);
      } else {
        jt++;
      }
    }
    if (it->second.empty()) {
      it = matrix.erase(it);
    } else {
      it++;
    }
  }
  return removed;
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
  for (auto it = data.deps.begin(); it != data.deps.end(); it++) {
    // Suppose, popularity is useless
    os << it->first << kSep << it->second.size() << kSep << /*popularity=*/0
       << std::endl;
    for (const auto jt : it->second) {
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
    // Suppose, popularity is useless
    // res.popularity[id] = popularity;
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

Data ConstructData(std::vector<User> &&users, int tread_id) {
  std::cout << "Thread " << tread_id << " spawned at "
            << std::chrono::system_clock::now() << std::endl;
  Data tracks_deps;
  int cnt = 0;
  for (auto it = users.begin(); it < users.end(); it++) {
    const User &user = *it;
    for (int i = 0; i < static_cast<int>(user.tracks.size()); i++) {
      auto upper_bound =
          std::min(static_cast<int>(user.tracks.size()), i + kDepShift);
      for (int j = i; j < upper_bound; j++) {
        tracks_deps.deps[user.tracks[i]][user.tracks[j]] += kDepShift - (j - i);
      }
    }
    if (++cnt >= kCleanEvery) {
      std::cout << "Start clean batch " << tread_id << "; "
                << CalcSize(tracks_deps.deps) << "; "
                << std::chrono::system_clock::now() << std::endl;
      auto removed = Reduce(tracks_deps.deps, kSaveThreshold);
      std::cout << "After clean " << tread_id << ": " << removed << "; "
                << std::chrono::system_clock::now() << std::endl;
      cnt = 0;
    }
  }
  Reduce(tracks_deps.deps, kSaveThreshold);
  return tracks_deps;
}

void Merge(Data &data, const Data &new_data) {
  auto &deps = data.deps;
  for (auto it = new_data.deps.begin(); it != new_data.deps.end(); it++) {
    auto &tr_deps = deps[it->first];
    for (auto jt = it->second.begin(); jt != it->second.end(); jt++) {
      auto old_it = tr_deps.find(jt->first);
      // first, no new threshold
      if (old_it != tr_deps.end()) {
        old_it->second += jt->second;
      } else {
        tr_deps.insert(*jt);
      }
    }
  }
}

void MergeAndSave() {
  Data res;
  for (auto batch_id = 0; batch_id < kThreads; batch_id++) {
    std::cout << "Start merge batch_id " << batch_id << " at "
              << std::chrono::system_clock::now() << std::endl
              << std::flush;
    for (auto dump_id = 0; dump_id < 20; dump_id++) {
      auto filename =
          "r_data_" + std::to_string(batch_id) + "_" + std::to_string(dump_id);
      Merge(res, Load(filename));
    }
  }
  Save(std::move(res), "r_merged_5kk");
}

std::vector<User> ReadData(const std::string &filename, int reserve) {
  std::vector<User> users;
  users.reserve(reserve);
  std::ifstream is(filename);
  std::string line;
  while (std::getline(is, line)) {
    users.push_back(ParseUser(line));
  }
  return users;
}

std::vector<User> ReadTrain() {
  auto fut1 = std::async(std::launch::async, ReadData,
                         std::string("data_train_5kk.yson"), 5000000);
  auto fut2 = std::async(std::launch::async, ReadData,
                         std::string("data_train_4kk.yson"), 4000000);
  auto users1 = fut1.get();
  auto users2 = fut2.get();
  users1.insert(users1.end(), std::make_move_iterator(users2.begin()),
                std::make_move_iterator(users2.end()));
  users2.clear();
  return users1;
}

Data TrainHard() {
  auto fut3 = std::async(std::launch::async, ReadData,
                         std::string("data_test.yson"), 1105889);
  auto train = ReadTrain();
  auto test = fut3.get();
  std::cout << "read tasks done at " << std::chrono::system_clock::now()
            << std::endl;
  size_t shift = 2500000;
  auto begin = train.begin();
  auto end = train.begin() + shift;
  std::vector<User> v1(std::make_move_iterator(begin),
                       std::make_move_iterator(end));
  begin += shift;
  end += shift;
  std::vector<User> v2(std::make_move_iterator(begin),
                       std::make_move_iterator(end));
  begin += shift;
  end += shift;
  std::vector<User> v3(std::make_move_iterator(begin),
                       std::make_move_iterator(end));
  begin += shift;
  std::vector<User> v4(std::make_move_iterator(begin),
                       std::make_move_iterator(train.end()));
  v4.insert(v4.begin(), std::make_move_iterator(test.begin()),
            std::make_move_iterator(test.end()));
  test.clear();
  train.clear();

  std::cout << "slice vector done at " << std::chrono::system_clock::now()
            << std::endl;

  auto train_fut_1 =
      std::async(std::launch::async, ConstructData, std::move(v1), 0);
  auto train_fut_2 =
      std::async(std::launch::async, ConstructData, std::move(v2), 1);
  auto train_fut_3 =
      std::async(std::launch::async, ConstructData, std::move(v3), 2);
  auto train_fut_4 =
      std::async(std::launch::async, ConstructData, std::move(v4), 3);

  auto data = train_fut_1.get();
  std::cout << "Merge 1 at " << std::chrono::system_clock::now() << std::endl;
  Merge(data, train_fut_2.get());
  std::cout << "Merge 1 at " << std::chrono::system_clock::now() << std::endl;
  Merge(data, train_fut_3.get());
  std::cout << "Merge 1 at " << std::chrono::system_clock::now() << std::endl;
  Merge(data, train_fut_4.get());

  std::cout << "Save at " << std::chrono::system_clock::now() << std::endl;
  Save(data, "r_data_all");

  return data;
}

int main() {
  std::cout << "started at " << std::chrono::system_clock::now() << std::endl;
  TrainHard();
  // TODO: predict
  std::cout << "finished at " << std::chrono::system_clock::now() << std::endl;
  return 0;
}
