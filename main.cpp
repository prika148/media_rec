#include <algorithm>
#include <chrono>
#include <fstream>
#include <future>
#include <iostream>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "date.h"

namespace {
const int kDepShift = 100;
const int kThreads = 8;
const int kCleanEvery = 100000;
int kSaveThreshold = 20;
} // namespace

using namespace date;

using IdT = unsigned int;
using SparseMatrix = std::unordered_map<IdT, std::unordered_map<IdT, int>>;

struct Data {
  SparseMatrix deps;
};

struct ScoredTrackId {
  IdT track_id;
  int score;
};

using DataIndex = std::unordered_map<IdT, std::vector<ScoredTrackId>>;
struct User {
  IdT id;
  std::vector<IdT> tracks{};
};

struct Prediction {
  IdT user_id;
  std::vector<IdT> prediction;
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
      if (dep_tr_id != id)
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

Data ConstructData(std::vector<User> &&users, int tread_id,
                   IdT *start_from_opt) {
  std::cout << "Thread " << tread_id << " spawned at "
            << std::chrono::system_clock::now() << std::endl;
  Data tracks_deps;
  int cnt = 0;
  bool start_found = !start_from_opt;
  for (auto it = users.begin(); it < users.end(); it++) {
    const User &user = *it;
    if (!start_found) {
      if (user.id == *start_from_opt) {
        start_found = true;
        tracks_deps = Load("r_data_big");
      } else {
        continue;
      }
    }
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
      Save(tracks_deps, "r_data_big.tmp");
      system("mv r_data_big.tmp r_data_big");
      std::cout << user.id << " Saved at " << std::chrono::system_clock::now()
                << std::endl;
      cnt = 0;
    }
  }
  Reduce(tracks_deps.deps, kSaveThreshold);
  std::cout << "Thread " << tread_id << " done at "
            << std::chrono::system_clock::now() << std::endl;
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

std::vector<User> ReadData(const std::string &filename, int reserve = 0) {
  std::vector<User> users;
  if (reserve)
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

Data TrainHard(IdT *start_from_opt) {
  auto fut3 = std::async(std::launch::async, ReadData,
                         std::string("data_test.yson"), 1105889);
  auto train = ReadTrain();
  auto test = fut3.get();
  std::cout << "read tasks done at " << std::chrono::system_clock::now()
            << std::endl;
  train.insert(train.begin(), std::make_move_iterator(test.begin()),
               std::make_move_iterator(test.end()));
  test.clear();

  auto train_fut = std::async(std::launch::async, ConstructData,
                              std::move(train), 0, start_from_opt);

  while (train_fut.wait_for(std::chrono::seconds(0)) !=
         std::future_status::ready) {
    int thold;
    std::cout << "Change thold:" << std::endl;
    std::cin >> thold;
    kSaveThreshold = thold;
  }
  auto data = train_fut.get();

  std::cout << "Save at " << std::chrono::system_clock::now() << std::endl;
  Save(data, "r_data_big.tmp");
  system("mv r_data_big.tmp r_data_big");

  return data;
}

std::vector<ScoredTrackId> Convert(std::unordered_map<IdT, int> &map) {
  std::vector<ScoredTrackId> vec;
  for (const auto &jt : map) {
    vec.push_back({jt.first, jt.second});
  }
  std::sort(vec.begin(), vec.end(),
            [](const ScoredTrackId &lhs, const ScoredTrackId &rhs) {
              return lhs.score > rhs.score;
            });
  map.clear();
  return vec;
}

DataIndex BuildIndex(Data &&data) {
  DataIndex result;
  for (auto &it : data.deps) {
    result[it.first] = Convert(it.second);
  }
  return result;
}

Prediction Predict(const DataIndex &data1, const User &user, int &trivials) {
  std::unordered_map<IdT, int> pretendents;
  std::vector<IdT> tracks_tmp;
  if (user.tracks.size() > kDepShift) {
    tracks_tmp.insert(tracks_tmp.begin(), user.tracks.end() - kDepShift,
                      user.tracks.end());
  }
  std::unordered_set<IdT> seen(user.tracks.begin(), user.tracks.end());
  for (const auto &track_id : user.tracks) {
    auto it = data1.find(track_id);
    if (it != data1.end()) {
      int cnt = 0;
      for (auto scored : it->second) {
        if (seen.count(scored.track_id))
          continue;
        pretendents[scored.track_id] += scored.score;
        if (cnt++ >= kDepShift) {
          break;
        }
      }
    }
  }
  auto sorted_pr = Convert(pretendents);
  Prediction result{user.id};
  auto end = std::min(sorted_pr.begin() + 100, sorted_pr.end());
  for (auto it = sorted_pr.begin(); it != end; it++) {
    result.prediction.push_back(it->track_id);
  }
  if (result.prediction.empty()) {
    trivials++;
    static const std::vector<IdT> kDummy = ([]() {
      std::vector<IdT> res;
      for (int i = 0; i < 100; i++)
        res.push_back(i);
      return res;
    })();
    result.prediction = kDummy;
  }
  return result;
}

DataIndex LoadIndex(const std::string &filename) {
  return BuildIndex(Load(filename));
}

void SavePredictions(std::vector<Prediction> &&predictions,
                     const std::string &filename) {
  std::ofstream os(filename);
  for (const auto &user : predictions) {
    os << "{\"user_id\":" << user.user_id << ", \"prediction\":\"";
    if (user.prediction.empty()) {
      os.close();
      throw std::runtime_error("!!! Empty predictions for " +
                               std::to_string(user.user_id));
    }
    auto it = user.prediction.begin();
    os << *it;
    it++;
    for (; it != user.prediction.end(); it++) {
      os << "\\t" << *it;
    }
    os << "\"}" << std::endl;
  }
  os.close();
}

int PredictAll() {
  std::cout << "started at " << std::chrono::system_clock::now() << std::endl;
  auto index1 = LoadIndex("r_data_big");
  std::cout << "Index loaded " << std::chrono::system_clock::now() << std::endl;
  auto users = ReadData("data_test.yson");
  std::cout << "Finish read data at " << std::chrono::system_clock::now()
            << std::endl;
  int cnt = 0;
  std::vector<Prediction> predictions;
  predictions.reserve(users.size());
  int trivials = 0;
  for (const auto &user : users) {
    predictions.push_back(Predict(index1, user, trivials));
    if (++cnt % 1000 == 0) {
      std::cout << "user " << cnt << ", trivials: " << trivials << "; at "
                << std::chrono::system_clock::now() << std::endl;
    }
  }
  std::cout << "All predicted, sz = " << predictions.size()
            << ", trivials: " << trivials << "at "
            << std::chrono::system_clock::now() << std::endl;
  SavePredictions(std::move(predictions), "predicted.json");
  std::cout << "finished at " << std::chrono::system_clock::now() << std::endl;
  return 0;
}

int main(int argc, char **argv) {
  IdT start_from;
  IdT *start_from_opt = nullptr;
  if (argc > 1) {
    if (std::string{"--train-from"} == argv[1]) {
      start_from = static_cast<IdT>(std::stoi(argv[2]));
      start_from_opt = &start_from;
    } else if (std::string{"--predict"} == argv[1]) {
      return PredictAll();
    }
  }
  TrainHard(start_from_opt);
  return PredictAll();
}
