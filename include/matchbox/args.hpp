#pragma once
#include <map>
#include <string>
#include <vector>

namespace matchbox {

// Minimal `--key value` / `--key=value` / `--flag` parser.
class Args {
 public:
  Args(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
      std::string a = argv[i];
      if (a.rfind("--", 0) != 0) {
        pos_.push_back(a);
        continue;
      }
      auto eq = a.find('=');
      if (eq != std::string::npos) {
        kv_[a.substr(2, eq - 2)] = a.substr(eq + 1);
      } else if (i + 1 < argc && argv[i + 1][0] != '-') {
        kv_[a.substr(2)] = argv[++i];
      } else {
        kv_[a.substr(2)] = "1";
      }
    }
  }
  std::string str(const std::string& k, const std::string& d) const {
    auto it = kv_.find(k);
    return it == kv_.end() ? d : it->second;
  }
  long num(const std::string& k, long d) const {
    auto it = kv_.find(k);
    return it == kv_.end() ? d : std::stol(it->second);
  }
  double real(const std::string& k, double d) const {
    auto it = kv_.find(k);
    return it == kv_.end() ? d : std::stod(it->second);
  }
  bool has(const std::string& k) const { return kv_.count(k) != 0; }
  const std::vector<std::string>& positional() const { return pos_; }

 private:
  std::map<std::string, std::string> kv_;
  std::vector<std::string> pos_;
};

}  // namespace matchbox
