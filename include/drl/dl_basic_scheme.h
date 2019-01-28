//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 1/27/19.
//

#ifndef DRL_DL_BASIC_SCHEME_H
#define DRL_DL_BASIC_SCHEME_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <functional>


namespace drl {


    template<typename _RMQ, typename _IsReported, typename _Report>
    void list(std::size_t _sp, std::size_t _ep, const _RMQ &_rmq, const _IsReported &_is_reported, _Report &_report) {
        auto k = _rmq(_sp, _ep);

        if (!_is_reported(k)) {
            _report(k);
            list(_sp, k - 1, _rmq, _is_reported, _report);
            list(k + 1, _ep, _rmq, _is_reported, _report);
        }
    }


    template<typename _RMQ, typename _IsReported, typename _Report>
    class DLBasicScheme {
    public:
        DLBasicScheme(const _RMQ &_rmq, const _IsReported &_is_reported, _Report &_reporter) : rmq_(_rmq),
                                                                                               is_reported_{
                                                                                                       _is_reported},
                                                                                               report_{_reporter} {}

        auto operator()(std::size_t _sp, std::size_t _ep) {
            std::vector<uint32_t> docs;
            auto add_doc = [&docs](auto d) { docs.push_back(d); };

            auto report = [this, &add_doc](const auto &k) { report_(k, add_doc); };

            list(_sp, _ep, rmq_, is_reported_, report);
        }

    private:
        const _RMQ &rmq_;
        const _IsReported &is_reported_;
        _Report &report_;
    };

}
#endif //DRL_DL_BASIC_SCHEME_H
