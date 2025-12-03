When performing a code review, don't complain about calls to unmanaged code. The project is all about bridging with an unmanaged library, so such calls are expected and necessary.

When performing a code review, focus (among other things) on the following aspects:
- Valid memory management practices when passing data to and from unmanaged code. Lifetimes of objects should be properly handled to avoid memory leaks or dangling pointers.
- Correctness of data marshaling between managed and unmanaged environments. Ensure that data types are appropriately converted and that there are no mismatches that could lead to runtime errors.
- Proper error handling around unmanaged code calls. Ensure that any potential errors from the unmanaged library are caught and handled gracefully in the managed code.
- Performance considerations when interacting with unmanaged code. Look for any unnecessary overhead or inefficiencies.
- Suggest using unsafe code blocks and unsafe functionality in C# when performance is critical and when dealing with pointers to unmanaged memory. We are open to using unsafe code where it makes sense for performance and interoperability.