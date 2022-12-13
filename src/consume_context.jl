abstract type AbstractConsumeContext end
abstract type AbstractTaskLocalConsumeContext <: AbstractConsumeContext end

function setup_tasks! end
function consume! end
function task_done! end
function sync_tasks end
function cleanup end

maybe_deepcopy(x::AbstractConsumeContext) = x
maybe_deepcopy(x::AbstractTaskLocalConsumeContext) = deepcopy(x)

struct DebugContext <: AbstractConsumeContext; end

function consume!(consume_ctx::DebugContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    io = IOBuffer()
    write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.tapeidxs), ", $(Base.current_task()) "))
    printstyled(IOContext(io, :color => true), "❚", color=Int(hash(Base.current_task()) % UInt8))
    println(io)
    @info String(take!(io))
    return nothing
end


function setup_tasks!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks = ntasks
    end
end
function task_done!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, result_buf::TaskResultBuffer)
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks -= 1
        notify(parsing_ctx.cond.cond_wait)
    end
end
function sync_tasks(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    @lock parsing_ctx.cond.cond_wait begin
        while true
            parsing_ctx.cond.ntasks == 0 && break
            wait(parsing_ctx.cond.cond_wait)
        end
    end
end
cleanup(consume_ctx::AbstractConsumeContext, e::Exception) = nothing

struct SkipContext <: AbstractConsumeContext
    SkipContext() = new()
end
function consume!(consume_ctx::SkipContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    return nothing
end


struct ValueExtractionContext <: AbstractConsumeContext
end

function consume!(consume_ctx::ValueExtractionContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    tape = task_buf.tape
    buf = parsing_ctx.bytes
    objinds = Dict{Symbol,Int}()
    arrinds = Int[]
    @inbounds for tapeidx in task_buf.tapeidxs
        t = tape[tapeidx]
        if JSON3.isobject(t)
            empty!(objinds)
            val = JSON3.Object(buf, Base.unsafe_view(tape, tapeidx:tapeidx + JSON3.getnontypemask(t)), objinds)
            JSON3.populateinds!(val::JSON3.Object)
            val
        elseif JSON3.isarray(t)
            empty!(arrinds)
            T = JSON3.Array{JSON3.geteltype(tape[tapeidx+1])}
            val = T(buf, Base.unsafe_view(tape, tapeidx:tapeidx + JSON3.getnontypemask(t)), arrinds)
            JSON3.populateinds!(val::T)
            val
        elseif JSON3.isstring(t)
            val = JSON3.getvalue(String, buf, tape, tapeidx, t)
            val
        elseif JSON3.isint(t)
            val = JSON3.getvalue(Int64, buf, tape, tapeidx, t)
            val
        elseif JSON3.isfloat(t)
            val = JSON3.getvalue(Float64, buf, tape, tapeidx, t)
            val
        elseif JSON3.isbool(t)
            val = JSON3.getvalue(Bool, buf, tape, tapeidx, t)
            val
        elseif JSON3.isnull(t)
            val = nothing
            val
        else
            @assert false "unreachable type reached (t = $t)"
        end
        @info val
    end
end
