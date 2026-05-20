#pragma once

#include <cstdint>

extern "C" {
#include "postgres.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

namespace pg_yaap {

template <uint16_t Capacity> struct DataChunk;

constexpr uint16_t PIPELINE_DEFAULT_CHUNK_SIZE = 2048;

namespace pipeline {

struct PipelineSharedControl;

using PipelineChunk = DataChunk<PIPELINE_DEFAULT_CHUNK_SIZE>;

enum class OperatorResultType : uint8_t {
	NEED_MORE_INPUT,
	HAVE_MORE_OUTPUT,
	FINISHED,
	BLOCKED,
};

enum class SourceResultType : uint8_t {
	HAVE_MORE_OUTPUT,
	FINISHED,
	BLOCKED,
};

enum class SinkResultType : uint8_t {
	NEED_MORE_INPUT,
	FINISHED,
	BLOCKED,
};

enum class SinkCombineResultType : uint8_t {
	FINISHED,
	BLOCKED,
};

enum class SinkFinalizeType : uint8_t {
	READY,
	NO_OUTPUT_POSSIBLE,
	BLOCKED,
};

using PipelineId = uint16_t;
constexpr PipelineId INVALID_PIPELINE_ID = static_cast<PipelineId>(-1);
using EventId = uint32_t;
constexpr EventId INVALID_EVENT_ID = static_cast<EventId>(-1);
constexpr int LEADER_WORKER_INDEX = -1;

struct ExecCtx {
	MemoryContext  mcxt;
	dsa_area      *dsa;
	int            worker_index;
	PipelineSharedControl *control = nullptr;
	EventId        profile_event_id = INVALID_EVENT_ID;
};

}
}
