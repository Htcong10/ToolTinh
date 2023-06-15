using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace busInputDataReading
{
    public class CreateHttpClient
    {
        private readonly HttpClient httpClient;
        public CreateHttpClient()
        {
            httpClient = new HttpClient();
        }

        public async Task<T> PostDataJsonAsync<T>(string body,string url,string strMaSoGCS)
        {
                var content = new StringContent(body, Encoding.ASCII, "application/json");
                httpClient.DefaultRequestHeaders.Accept.Clear();
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                using (HttpResponseMessage response = await httpClient.PostAsync(url, content))
                {
                    string serialized = string.Empty;
                    try
                    {
                        serialized = await response.Content.ReadAsStringAsync();
                        if (typeof(T) == typeof(string))
                        {
                            return (T)(object)serialized;
                        }
                        else
                        {
                            Console.WriteLine("Lấy xong dữ liệu cho luồng " + strMaSoGCS);
                            return JsonConvert.DeserializeObject<T>(serialized);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }           
        }
    }
}
