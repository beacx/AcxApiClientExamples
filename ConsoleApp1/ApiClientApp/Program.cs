using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ApiClientApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Configuration dictionary holding client credentials and API endpoint information.
            // CLIENT_SECRET should be securely managed and not hardcoded in production.
            var config = new Dictionary<string, string>
            {
                {"CLIENT_ID", ""},
                {"CLIENT_SECRET", ""}, // TODO: Replace with your actual client secret
                {"SCOPE", "acxapi"},
                {"API_URL", "https://api.beauthenticx.com"}, 
                {"ORG_ID", ""}
            };

            var httpClient = new HttpClient();

            // Retrieve an access token for API auth.
            var token = await GetAccessToken(httpClient, config);
            if (string.IsNullOrEmpty(token))
            {
                Console.WriteLine("Failed to retrieve access token.");
                return;
            }

            // Set the authorization header for subsequent API requests.
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            // Calculate the time window for metadata retrieval.
            // This example retrieves metadata from the previous hour.
            var utcNow = DateTime.UtcNow;
            var oneHourAgoOnTheDot = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, 0, 0, DateTimeKind.Utc).AddHours(-1);
            var nowString = oneHourAgoOnTheDot.ToString("o");

            // This example hardcodes a specific start time.
            // var startTime = DateTime.Parse(nowString, null, System.Globalization.DateTimeStyles.AdjustToUniversal);
            var startTime = DateTime.Parse("2025-02-05T21:10:00",null, System.Globalization.DateTimeStyles.AdjustToUniversal);
            var endTime = startTime.AddMinutes(1); // Example uses 1 minute window, but recommend using 1 hour.

            // Retrieve metadata objects within the specified time window.
            var metadataObjects = await GetMetadata(httpClient, config["API_URL"], startTime, endTime);

            // Parallel patch requests example, not necessary.
            var semaphore = new SemaphoreSlim(5); // Limit to 5 parallel requests
            var tasks = new List<Task>();

            // Iterate through the retrieved metadata objects and create a task to patch each one.
            foreach (var obj in metadataObjects)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await semaphore.WaitAsync(); // Wait for an available slot in the semaphore.
                    try
                    {
                        Console.WriteLine($"Patching metadata for ID: {obj.Id}");
                        // Patch the metadata for the current object.
                        await PatchMetadata(httpClient, config["API_URL"], obj.Id);
                    }
                    finally
                    {
                        semaphore.Release(); // Release the semaphore slot.
                    }
                }));
            }

            // Wait for all patching tasks to complete.
            await Task.WhenAll(tasks);
        }

        // Asynchronously retrieves an access token from the authentication server.
        static async Task<string> GetAccessToken(HttpClient client, Dictionary<string, string> config)
        {
            var tokenEndpoint = $"{config["API_URL"]}/connect/token";
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("client_id", config["CLIENT_ID"]),
                new KeyValuePair<string, string>("client_secret", config["CLIENT_SECRET"]),
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("scope", config["SCOPE"])
            });

            var response = await client.PostAsync(tokenEndpoint, content);
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine("Token request failed: " + await response.Content.ReadAsStringAsync());
                return null;
            }

            var payload = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            // Extracts and returns the access token from the JSON response.
            return payload.RootElement.GetProperty("access_token").GetString();
        }

        // Asynchronously retrieves a list of metadata items from the API within a specified time range.
        // Handles pagination to retrieve all items if the result set is larger than a single page.
        static async Task<List<MetadataItem>> GetMetadata(HttpClient client, string apiUrl, DateTime start, DateTime end)
        {
            var metadataList = new List<MetadataItem>();
            string lastId = null; // Used for pagination, stores the ID of the last retrieved item.
            bool hasMore = true; // Flag to indicate if there are more pages of data to retrieve.

            while (hasMore)
            {
                // Construct the API URL with query parameters for start date, end date, page size, and pagination (lastId).
                var urlBuilder = new StringBuilder($"{apiUrl}/metadata?startDate={Uri.EscapeDataString(start.ToString("o"))}&endDate={Uri.EscapeDataString(end.ToString("o"))}&pageSize=20");
                if (!string.IsNullOrEmpty(lastId))
                {
                    urlBuilder.Append($"&lastId={lastId}");
                }

                var response = await client.GetAsync(urlBuilder.ToString());

                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine("Metadata retrieval failed: " + await response.Content.ReadAsStringAsync());
                    break; // Exit the loop if the API request fails.
                }

                var contentString = await response.Content.ReadAsStringAsync();
                // Deserialize the JSON response into a list of MetadataItem objects.
                var metadataResponse = JsonSerializer.Deserialize<List<MetadataItem>>(contentString);

                if (metadataResponse != null &&  metadataResponse.Count > 0)
                {
                    metadataList.AddRange(metadataResponse);
                    lastId = metadataResponse[^1].Id; // Update lastId for the next iteration if pagination is needed.
                }
                else
                {
                    hasMore = false; // No more data to retrieve.
                }
            }

            return metadataList;
        }

        // Asynchronously patches metadata for a specific item ID.
        // This method uses an ExecuteWithRetry strategy to handle transient network issues.
        static async Task PatchMetadata(HttpClient client, string apiUrl, string id)
        {
            await ExecuteWithRetry(async () =>
            {
                string url = $"{apiUrl}/metadata/{id}";

                // Define the payload for the PATCH request.
                // This example adppends extended metadata for a Converation to include "MetaPropertyX" field and hardcoded value.
                var patchPayload = new
                {
                    extendedMetadata = new Dictionary<string, string>
                    {
                        { "MetaPropertyX", "valueY" }
                    },
                };

                var serializedPayload = JsonSerializer.Serialize(patchPayload);
                var request = new HttpRequestMessage(new HttpMethod("PATCH"), url)
                {
                    Content = new StringContent(serializedPayload, Encoding.UTF8, "application/json")
                };
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = await client.SendAsync(request);

                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Patch failed for {id}: {await response.Content.ReadAsStringAsync()}");
                    throw new Exception($"Patch failed for {id}: {await response.Content.ReadAsStringAsync()}");
                }
                else
                {
                    Console.WriteLine($"Successfully patched {id}.");
                }
                return response;
            });
        }
        
        // Semaphore to ensure thread safety for the retry logic, allowing only one retry operation at a time.
        static readonly SemaphoreSlim RetrySemaphore = new SemaphoreSlim(1, 1);

        // Executes a given asynchronous action with a retry mechanism.
        // Retries the action up to specified "maxRetries" times with an exponential backoff delay.
        static async Task<T> ExecuteWithRetry<T>(Func<Task<T>> action, int maxRetries = 5, int initialDelayMilliseconds = 1000)
        {
            int attempt = 0;
            int delay = initialDelayMilliseconds;

            await RetrySemaphore.WaitAsync(); // Ensure only one thread executes the retry logic at a time.
            try
            {
                while (true)
                {
                    try
                    {
                        return await action();
                    }
                    catch (Exception ex)
                    {
                        attempt++;
                        if (attempt >= maxRetries)
                        {
                            Console.WriteLine($"Max retries reached. Skipping this operation.");
                            throw; // Re-throw the exception if max retries are exceeded.
                        }

                        Console.WriteLine($"Attempt {attempt} failed: {ex.Message}. Retrying in {delay}ms...");
                        await Task.Delay(delay); // Wait before the next retry.
                        delay *= 2; // Double the delay for exponential backoff.
                    }
                }
            }
            finally
            {
                RetrySemaphore.Release();
            }
        }
    }
    
// Represents metadata retrieved from the API for a Conversation.
public class MetadataItem
{
    [JsonPropertyName("id")]
    public string Id { get; set; }

    // [JsonPropertyName("acxIdentifier")]
    // public string AcxIdentifier { get; set; }
    //
    // [JsonPropertyName("fileName")]
    // public string FileName { get; set; }
    //
    // [JsonPropertyName("origFileName")]
    // public string OrigFileName { get; set; }
    //
    // [JsonPropertyName("status")]
    // public string Status { get; set; }
    //
    // [JsonPropertyName("timestamp")]
    // public DateTime Timestamp { get; set; }
    //
    // [JsonPropertyName("phone")]
    // public string Phone { get; set; }
    //
    // [JsonPropertyName("agentName")]
    // public string AgentName { get; set; }
    //
    // [JsonPropertyName("callDirection")]
    // public string CallDirection { get; set; }
    //
    // [JsonPropertyName("callDurationMillis")]
    // public int CallDurationMillis { get; set; }
    //
    // [JsonPropertyName("fileSizeKiloBytes")]
    // public int FileSizeKiloBytes { get; set; }
    //
    // [JsonPropertyName("clientCallId")]
    // public string ClientCallId { get; set; }
    //
    // [JsonPropertyName("organizationId")]
    // public string OrganizationId { get; set; }
    //
    // [JsonPropertyName("organizationStructureMemberId")]
    // public string OrganizationStructureMemberId { get; set; }
    //
    // [JsonPropertyName("arrivedOn")]
    // public DateTime ArrivedOn { get; set; }
    //
    // [JsonPropertyName("mediaType")]
    // public string MediaType { get; set; }
    //
    // [JsonPropertyName("meta1")]
    // public string Meta1 { get; set; }
    //
    // [JsonPropertyName("meta2")]
    // public string Meta2 { get; set; }
    //
    // [JsonPropertyName("meta3")]
    // public string Meta3 { get; set; }
    //
    // [JsonPropertyName("meta4")]
    // public string Meta4 { get; set; }
    //
    // [JsonPropertyName("meta5")]
    // public string Meta5 { get; set; }
    //
    // [JsonPropertyName("extendedMetadata")]
    // public List<ExtendedMetadataItem> ExtendedMetadata { get; set; }
    //
    // [JsonPropertyName("extendedMetadataValues")]
    // public Dictionary<string, string> ExtendedMetadataValues { get; set; }
    //
    // [JsonPropertyName("processFailures")]
    // public List<string> ProcessFailures { get; set; }
    //
    // [JsonPropertyName("uploadedFileName")]
    // public string UploadedFileName { get; set; }
    //
    // [JsonPropertyName("localeId")]
    // public int LocaleId { get; set; }
}

    // Class representing a slim metadata object, as Id is the only property needed.
    public class MetadataObject
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }
    }

    
}

