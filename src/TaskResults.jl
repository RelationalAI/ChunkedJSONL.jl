struct TaskResultBuffer
    id::Int
    tape::Vector{UInt64}
    tapeidxs::BufferedVector{UInt32}
    arrinds::Vector{Int}
    objinds::Dict{Symbol,Int}
end

TaskResultBuffer(id::Int) = TaskResultBuffer(id, UInt64[], BufferedVector{UInt32}(), Int[], Dict{Symbol,Int}())
TaskResultBuffer(id::Int, n) = TaskResultBuffer(id, UInt64[], BufferedVector{UInt32}(Vector{UInt32}(undef, n), 0), Int[], Dict{Symbol,Int}())


function Base.empty!(buf::TaskResultBuffer)
    empty!(buf.tapeidxs)
    empty!(buf.tape)
    return buf
end

function Base.ensureroom(buf::TaskResultBuffer, n)
    Base.ensureroom(buf.tapeidxs, n)
    return buf
end
