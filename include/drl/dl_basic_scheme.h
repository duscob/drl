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


template<typename _GetDoc, typename _Reported, typename _GetDocs>
class DLBasicILCP {
 public:
  DLBasicILCP(const std::shared_ptr<CSA::DeltaVector> &_run_heads,
              _GetDoc &_get_doc,
              std::size_t _nd,
              _GetDocs &_get_docs) : run_heads_{
      _run_heads}, get_doc_{_get_doc}, reported_{_nd, 0}, get_docs_{_get_docs} {}

  /// Get document
  auto operator()(std::size_t _k) const {
    CSA::DeltaVector::Iterator iter(*run_heads_);

    auto b = std::max(sp_, iter.select(_k));
    return get_doc_(b);
  }

  /// Is reported?
  bool operator()(std::size_t _k, uint32_t _d) const {
    return reported_[_d];
  }

  /// Report documents
  template<typename _Report>
  void operator()(std::size_t _k, uint32_t _d, _Report &_report_doc) {
    auto report = [&_report_doc, this](auto __d) {
      _report_doc(__d);
      reported_[__d] = 1;
    };

    report(_d);

    CSA::DeltaVector::Iterator iter(*run_heads_);

    auto b = std::max(sp_, iter.select(_k)) + 1;
    auto e = std::min(ep_, iter.selectNext() - 1);
    if (e < b) return;

    get_docs_(b, e, report);
  }

  void setInitialRange(std::size_t _sp, std::size_t _ep) {
    sp_ = _sp;
    ep_ = _ep;
  }

  const auto &getRunHeads() const {
    return run_heads_;
  }

 protected:
  std::shared_ptr<CSA::DeltaVector> run_heads_;
  const _GetDoc &get_doc_;

  _Reported reported_;

  _GetDocs &get_docs_;

  std::size_t sp_ = 0;
  std::size_t ep_ = 0;
};


template<typename _DLBasicILCP>
class PreprocessILCP {
 public:
  PreprocessILCP(_DLBasicILCP &_get_docs_ilcp) : get_docs_ilcp_{_get_docs_ilcp} {}

  void operator()(std::size_t &_sp, std::size_t &_ep) {
    get_docs_ilcp_.setInitialRange(_sp, _ep);

    CSA::DeltaVector::Iterator iter(*get_docs_ilcp_.getRunHeads());
    _sp = iter.rank(_sp) - 1;
    _ep = iter.rank(_ep) - 1;
  }

 private:
  _DLBasicILCP &get_docs_ilcp_;
};


template<typename _RMQ, typename _GetDoc, typename _Reported, typename _GetDocs>
class DLILCP
    : public DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
      public DLBasicScheme<_RMQ,
                           DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                           DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                           DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                           PostprocessCleanReported<_Reported>,
                           PreprocessILCP<DLBasicILCP<_GetDoc, _Reported, _GetDocs>>> {
 public:
  DLILCP(const _RMQ &_rmq,
         const std::shared_ptr<CSA::DeltaVector> &_run_heads,
         _GetDoc &_get_doc,
         std::size_t _nd,
         _GetDocs &_get_docs)
      : DLBasicILCP<_GetDoc, _Reported, _GetDocs>{_run_heads, _get_doc, _nd, _get_docs},
        DLBasicScheme<_RMQ,
                      DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                      DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                      DLBasicILCP<_GetDoc, _Reported, _GetDocs>,
                      PostprocessCleanReported<_Reported>,
                      PreprocessILCP<DLBasicILCP<_GetDoc, _Reported, _GetDocs>>>{
            _rmq, *this, *this, *this, PostprocessCleanReported<_Reported>{this->reported_},
            PreprocessILCP<DLBasicILCP<_GetDoc, _Reported, _GetDocs>>{*this}} {}
};


template<typename _Reported, typename _RMQ, typename _GetDoc, typename _GetDocs>
auto BuildDLILCP(const _RMQ &_rmq,
                 const std::shared_ptr<CSA::DeltaVector> &_run_heads,
                 _GetDoc &_get_doc,
                 std::size_t _nd,
                 _GetDocs &_get_docs) {
  return DLILCP<_RMQ, _GetDoc, _Reported, _GetDocs>{_rmq, _run_heads, _get_doc, _nd, _get_docs};
}

typedef sdsl::rmq_succinct_sct<true, sdsl::bp_support_sada<256, 32, sdsl::rank_support_v5<>>> DefaultRMQ;


class GetDocRLCSA {
 public:
  GetDocRLCSA(const std::shared_ptr<CSA::RLCSA> &_rlcsa) : rlcsa_{_rlcsa} {}

  auto operator()(std::size_t _k) const {
    return rlcsa_->getSequenceForPosition(rlcsa_->locate(_k));
  }

  template<typename _Report>
  void operator()(std::size_t _b, std::size_t _e, _Report &_report) const {
    pair_type range(_b, _e);

    usint *res = rlcsa_->locate(range);
    rlcsa_->getSequenceForPosition(res, CSA::length(range));

    for (usint i = range.first; i <= range.second; i++) {
      _report(res[i - range.first]);
    }

    delete[] res;
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

  template<typename _Report>
  void operator()(std::size_t _b, std::size_t _e, _Report &_report) const {
    for (auto i = _b; i <= _e; ++i) {
      _report(da_[i]);
    }
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

  template<typename _Report>
  void operator()(std::size_t _b, std::size_t _e, _Report &_report) const {
    auto l = _e - _b + 1;
    bool all = false;

    GetDocs(slp_.Start(), _b, l, _report, all);
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

  template<typename _Report>
  void GetDocs(uint32_t _i, std::size_t _k, std::size_t &_l, _Report &_report, bool &_all) const {
    if (0 == _l)
      return;

    if (slp_.IsTerminal(_i) && (_k == 0 || _all)) {
      _report(_i);

      --_l;
      _all = true;

      return;
    }

    auto children = slp_[_i];

    auto left_length = slp_.SpanLength(children.first);
    if (_k < left_length || _all)
      GetDocs(children.first, _k, _l, _report, _all);

    if (left_length <= _k || _all) {
      GetDocs(children.second, _k - left_length, _l, _report, _all);
    }
  }

 private:
  const _SLP &slp_;
};


template<typename _GetDoc>
class DefaultGetDocs {
 public:
  DefaultGetDocs(_GetDoc &_get_doc) : get_doc_{_get_doc} {}

  template<typename _Report>
  void operator()(std::size_t _b, std::size_t _e, _Report &_report_doc) const {
    for (auto i = _b; i <= _e; ++i) {
      auto d = get_doc_(i);
      _report_doc(d);
    }
  }

 private:
  _GetDoc &get_doc_;
};

}
#endif //DRL_DL_BASIC_SCHEME_H
