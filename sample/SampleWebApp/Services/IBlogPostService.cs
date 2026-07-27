using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EnyimRedux.Caching.SampleWebApp.Models;

namespace EnyimRedux.Caching.SampleWebApp.Services
{
    public interface IBlogPostService
    {
        ValueTask<Dictionary<string, List<BlogPost>>> GetRecent(int itemCount);
    }
}
