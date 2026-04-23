using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Enyim.Caching.Memcached;

public class SegmentedMemoryStream : IDisposable
{
    private int _length = 0;
    private int _chunkSize;
    private List<byte[]> _segments = new List<byte[]>();
    
    public int Length => _length;

    public SegmentedMemoryStream(int chunksize = 512)
    {
        _chunkSize = chunksize;
    }

    public void WriteByte(byte b)
    {
        if (_length % _chunkSize == 0)
        {
            _segments.Add(ArrayPool<byte>.Shared.Rent(_chunkSize));
        }

        _segments[_segments.Count - 1][_length % _chunkSize] = b;
        _length++;
    }

    public byte[] ToArray(byte[] arr = null)
    {
        if (arr == null)
            arr = new byte[_length];
        for(var i = 0; i < _segments.Count; i++)
        {
            var chunkMax = _chunkSize;
            if (i == _segments.Count - 1)
                chunkMax = _length % _chunkSize;
            for(var j = 0; j < chunkMax; j++)
                arr[(i * _chunkSize) + j] = _segments[i][j];
        }
        return arr;
    }

    public string ConvertToAscii()
    {
        var arr = ArrayPool<byte>.Shared.Rent(_length);
        var ret= Encoding.ASCII.GetString(ToArray(arr), 0, _length);
        ArrayPool<byte>.Shared.Return(arr);
        return ret;
    }

    public void Dispose()
    {
        for (var i = 0; i < _segments.Count; i++)
        {
            ArrayPool<byte>.Shared.Return(_segments[i]);
            _segments[i] = null;
        }
    }
}