//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 1/27/19.
//

#ifndef DRL_DL_BASIC_SCHEME_H
#define DRL_DL_BASIC_SCHEME_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <functional>

#include <sdsl/rmq_support.hpp>
#include <rlcsa/rlcsa.h>

namespace drl {

template<typename _RMQ, typename _GetDoc, typename _IsReported, typename _Report>
void list_docs(std::size_t _sp, std::size_t _ep, const _RMQ &_rmq, _GetDoc &_get_doc,
               const _IsReported &_is_reported, _Report &_report) {
  if (_sp > _ep) return;

  auto k = _rmq(_sp, _ep);
  auto d = _get_doc(k);

  if (!_is_reported(k, d)) {
    _report(k, d);
    list_docs(_sp, k - 1, _rmq, _get_doc, _is_reported, _report);
    list_docs(k + 1, _ep, _rmq, _get_doc, _is_reported, _report);
  }
}


template<typename _RMQ, typename _GetDoc, typename _IsReported, typename _Report, typename _Postprocess, typename _Preprocess>
class DLBasicScheme {
 public:
  DLBasicScheme(const _RMQ &_rmq, const _GetDoc &_get_doc, const _IsReported &_is_reported, _Report &_report,
                _Postprocess _postprocess, _Preprocess _preprocess)
      : rmq_(_rmq), get_doc_{_get_doc}, is_reported_{_is_reported}, report_{_report},
        postprocess_{_postprocess}, preprocess_{_preprocess} {}

  auto list(std::size_t _sp, std::size_t _ep) {
    std::vector<uint32_t> docs;
    auto add_doc = [&docs](auto d) { docs.push_back(d); };

    auto &report__ = this->report_;
    auto report = [&report__, &add_doc](auto _k, auto _d) { report__(_k, _d, add_doc); };

    preprocess_(_sp, _ep);

    list_docs(_sp, _ep, rmq_, get_doc_, is_reported_, report);

    postprocess_(docs);

    return docs;
  }

 protected:
  const _RMQ &rmq_;
  const _GetDoc &get_doc_;
  const _IsReported &is_reported_;
  _Report &report_;

  _Postprocess postprocess_;
  _Preprocess preprocess_;
};


class DefaultProcess {
 public:
  template<typename ..._Args>
  void operator()(const _Args &... _args) {}
};


template<typename _RMQ, typename _GetDoc, typename _IsReported, typename _Report, typename _Postprocess = DefaultProcess, typename _Preprocess = DefaultProcess>
auto BuildDLScheme(const _RMQ &_rmq, _GetDoc &_get_doc, const _IsReported &_is_reported, _Report &_report,
                   _Postprocess _postprocess = DefaultProcess(), _Preprocess _preprocess = DefaultProcess()) {
  return DLBasicScheme<_RMQ, _GetDoc, _IsReported, _Report, _Postprocess, _Preprocess>(
      _rmq, _get_doc, _is_reported, _report, _postprocess, _preprocess);
}


template<typename _Reported>
class IsReported {
 public:
  IsReported(_Reported &_reported) : reported_{_reported} {}

  bool operator()(std::size_t _k, uint32_t _d) const {
    return reported_[_d];
  }

 private:
  const _Reported &reported_;
};


template<typename _Reported>
class PostprocessCleanReported {
 public:
  PostprocessCleanReported(_Reported &_bitvector) : reported_{_bitvector} {}

  template<typename _Container>
  void operator()(const _Container &_result) {
    for (const auto &item: _result) {
      reported_[item] = 0;
    }
  }

 private:
  _Reported &reported_;
};


template<typename _Reported>
class DLBasicSadakane {
 public:
  DLBasicSadakane(std::size_t _nd) : reported_{_nd, 0} {}

  /// Is reported?
  bool operator()(std::size_t _k, uint32_t _d) const {
    return reported_[_d];
  }

  /// Report documents
  template<typename _Report>
  void operator()(std::size_t _k, uint32_t _d, _Report &_report_doc) {
    _report_doc(_d);

    reported_[_d] = 1;
  };

 protected:
  _Reported reported_;
};


template<typename _RMQ, typename _GetDoc, typename _Reported>
class DLSadakane
    : public DLBasicSadakane<_Reported>,
      public DLBasicScheme<_RMQ,
                           _GetDoc,
                           DLBasicSadakane<_Reported>,
                           DLBasicSadakane<_Reported>,
                           PostprocessCleanReported<_Reported>,
                           DefaultProcess> {
 public:
  DLSadakane(const _RMQ &_rmq, const _GetDoc &_get_doc, std::size_t _nd)
      : DLBasicSadakane<_Reported>{_nd},
        DLBasicScheme<_RMQ,
                      _GetDoc,
                      DLBasicSadakane<_Reported>,
                      DLBasicSadakane<_Reported>,
                      PostprocessCleanReported<_Reported>,
                      DefaultProcess>{_rmq, _get_doc, *this, *this,
                                      PostprocessCleanReported<_Reported>{this->reported_}, DefaultProcess{}} {}
};


template<typename _Reported, typename _RMQ, typename _GetDoc>
auto BuildDLSadakane(const _RMQ &_rmq, const _GetDoc &_get_doc, std::size_t _nd) {
  return DLSadakane<_RMQ, _GetDoc, _Reported>{_rmq, _get_doc, _nd};
}

typedef sdsl::rmq_succinct_sct<true, sdsl::bp_support_sada<256, 32, sdsl::rank_support_v5<>>> DefaultRMQ;


class GetDocRLCSA {
 public:
  GetDocRLCSA(const std::shared_ptr<CSA::RLCSA> &_rlcsa) : rlcsa_{_rlcsa} {}

  auto operator()(std::size_t _k) const {
    return rlcsa_->getSequenceForPosition(rlcsa_->locate(_k));
  }

 private:
  std::shared_ptr<CSA::RLCSA> rlcsa_;
};


template<typename _DA>
class GetDocDA {
 public:
  GetDocDA(_DA &_da) : da_{_da} {}

  auto operator()(std::size_t _k) const {
    return da_[_k];
  }

 private:
  const _DA &da_;
};


template<typename _SLP>
class GetDocGCDA {
 public:
  GetDocGCDA(_SLP &_slp) : slp_{_slp} {}

  auto operator()(std::size_t _k) const {
    return GetDoc(slp_.Start(), _k);
  }

  auto GetDoc(uint32_t _i, std::size_t _k) const {
    if (_k == 0 && slp_.IsTerminal(_i))
      return _i;

    auto children = slp_[_i];

    auto left_length = slp_.SpanLength(children.first);
    if (_k < left_length)
      return GetDoc(children.first, _k);
    else
      return GetDoc(children.second, _k - left_length);
  }

 private:
  const _SLP &slp_;
};

}
#endif //DRL_DL_BASIC_SCHEME_H
