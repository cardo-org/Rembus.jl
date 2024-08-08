#=
Copyright (c) 2016 Saurav Sachidanand

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

num2hex(n) = string(n, base=16, pad=sizeof(n) * 2)
num2hex(n::AbstractFloat) = num2hex(reinterpret(Unsigned, n))
hex2num(s) = reinterpret(Float64, parse(UInt64, s, base=16))
hex(n) = string(n, base=16)

struct Tag{T}
    id::Int
    data::T
end
Base.:(==)(a::Tag, b::Tag) = a.id == b.id && a.data == b.data
Tag(id::Integer, data) = Tag(Int(id), data)

export encode
export decode, decode_with_iana
export Undefined

function decode(data)
    return decode_internal(IOBuffer(data))
end

function decode(data::IO)
    return decode(read(data))
end

function encode(data)
    io = IOBuffer()
    encode(io, data)
    return take!(io)
end
