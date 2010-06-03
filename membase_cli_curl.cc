#include <iostream>
#include <string>
#include <"curl/curl.h">

using namespace std;

class MembaseCliCurl {
private:
  string url;
  // for writing all data to
  static string buffer;
  // Curl handle
  CURL *curl;

  // private callback method required for curl
  static int fcallback(char *data,
                      size_t size,
                      size_t nmemb,
                      string *buffer)
  {
    // return
    int result = 0;

    // Is buffer set already?
    if (buffer != NULL)
    {
      // append data to buffer
      buffer->append(data, size * nmemb);

      // size of what was written
      result = size * nmemb;
    }

    return result;
  }

public:
  // constructor
  MembaseCliCurl (char *url) {
    // create curl handle
    curl= curl_easy_init();

    if (curl) {
      // set up curl options
      curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
      curl_easy_setopt(curl, CURLOPT_URL, url);
      curl_easy_setopt(curl, CURLOPT_HEADER, 0);
      curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, fcallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
    }
  }

  // URL fetching method
  string accessUrl (string url) {
    static string buffer;
    CURLcode *result;

    // retrieve remote page
    result = curl_easy_perform(curl);

    // cleanup
    curl_easy_cleanup(curl);

    if (result == CURL_OK) {
      cout << buffer << endl;
      exit(0);
    }
    else {
      cout << "Error: [" << result << "] - " << errorBuffer;
      exit(-1);
    }
  }

  // destructor
  ~MembaseCliCurl () {
    delete curl;
  }
}
