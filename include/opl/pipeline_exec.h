#pragma once

#include <opl/detail/pipeline_exec.h>

namespace opl {

/**
 * @brief Generic small-step pipeline runtime for a `SubPipeline` stage.
 *
 * This is a public alias to the reference implementation in `include/opl/detail/*`.
 */
template <OpenPipelineTraits Traits>
using PipelineExec = detail::PipelineExec<Traits>;

}  // namespace opl

