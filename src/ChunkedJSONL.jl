module ChunkedJSONL

export parse_file, consume!, DebugContext, AbstractConsumeContext

using ScanByte
import JSON3
using .Threads: @spawn
using TranscodingStreams
using CodecZlib
using Mmap

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

include("BufferedVectors.jl")
include("TaskResults.jl")

include("exceptions.jl")

mutable struct TaskCondition
    ntasks::Int
    cond_wait::Threads.Condition
end
TaskCondition() = TaskCondition(0, Threads.Condition(ReentrantLock()))

struct ParsingContext
    bytes::Vector{UInt8}
    eols::BufferedVector{UInt32}
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
    cond::TaskCondition
end
function estimate_task_size(parsing_ctx::ParsingContext)
    length(parsing_ctx.eols) == 1 && return 1 # empty file
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, ceil(Int, last(parsing_ctx.eols)  / length(parsing_ctx.eols))))
    return max(min_rows, cld(ceil(Int, length(parsing_ctx.eols) * ((1 + length(parsing_ctx.bytes)) / (1 + last(parsing_ctx.eols)))), parsing_ctx.maxtasks))
end

include("read_and_lex.jl")
# include("init_parsing.jl")
include("consume_context.jl")

include("row_parsing.jl")
include("parser_serial.jl")
include("parser_singlebuffer.jl")
include("parser_doublebuffer.jl")


function parse_file(
    input,
    consume_ctx::AbstractConsumeContext=DebugContext();
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    nresults::Integer=maxtasks,
    use_mmap::Bool=false,
    _force::Symbol=:none,
)
    0 < buffersize < typemax(UInt32) || throw(ArgumentError("`buffersize` argument must be larger than 0 and smaller than 4_294_967_295 bytes."))
    nworkers > 0 || throw(ArgumentError("`nworkers` argument must be larger than 0."))
    maxtasks >= nworkers || throw(ArgumentError("`maxtasks` argument must be larger of equal to the `nworkers` argument."))
    # otherwise not implemented; implementation postponed once we know why take!/wait allocates
    nresults == maxtasks || throw(ArgumentError("Currently, `nresults` argument must be equal to the `maxtasks` argument."))

    should_close, io = _input_to_io(input, use_mmap)

    lexer_state = LexerState(io)
    parsing_ctx = ParsingContext(Vector{UInt8}(undef, buffersize), BufferedVector{UInt32}(), nworkers, maxtasks, nresults, TaskCondition())
    read_and_lex!(lexer_state, parsing_ctx)

    if _force === :doublebuffer
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx)
    elseif _force === :singlebuffer
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx)
    elseif _force === :serial || Threads.nthreads() == 1 || parsing_ctx.nworkers == 1 || parsing_ctx.maxtasks == 1 || lexer_state.last_newline_at < MIN_TASK_SIZE_IN_BYTES
              _parse_file_serial(lexer_state, parsing_ctx, consume_ctx)
    elseif !lexer_state.done
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx)
    else
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx)
    end
    # end
    should_close && close(io)
    return nothing
end


struct DummyBeTreeUpsertingContext <: AbstractConsumeContext
    acc::Vector{Any}
    lock::ReentrantLock
end
DummyBeTreeUpsertingContext() = DummyBeTreeUpsertingContext([], ReentrantLock())

function consume!(consume_ctx::DummyBeTreeUpsertingContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    tape = task_buf.tape
    buf = parsing_ctx.bytes
    out = 0
    @inbounds for tapeidx in task_buf.tapeidxs
        t = tape[tapeidx]
        if JSON3.isobject(t)
            empty!(task_buf.objinds)
            val = JSON3.Object(buf, Base.unsafe_view(tape, tapeidx:tapeidx + JSON3.getnontypemask(t)), task_buf.objinds)
            JSON3.populateinds!(val::JSON3.Object)
            out += length(val::JSON3.Object)
            val
        elseif JSON3.isarray(t)
            empty!(task_buf.arrinds)
            T = JSON3.Array{JSON3.geteltype(tape[tapeidx+1])}
            val = T(buf, Base.unsafe_view(tape, tapeidx:tapeidx + JSON3.getnontypemask(t)), task_buf.arrinds)
            JSON3.populateinds!(val::T)
            out += length(val::T)
            val
        elseif JSON3.isstring(t)
            val = JSON3.getvalue(String, buf, tape, tapeidx, t)
            out += length(val::String)
            val
        elseif JSON3.isint(t)
            val = JSON3.getvalue(Int64, buf, tape, tapeidx, t)
            out += length(val::Int)
            val
        elseif JSON3.isfloat(t)
            val = JSON3.getvalue(Float64, buf, tape, tapeidx, t)
            out += length(val::Float64)
            val
        elseif JSON3.isbool(t)
            val = JSON3.getvalue(Bool, buf, tape, tapeidx, t)
            out += length(val::Bool)
            val
        elseif JSON3.isnull(t)
            val = nothing
            out += length(val::Nothing)
            val
        end
    end
    return out
end


end # module
