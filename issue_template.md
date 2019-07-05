When encountering an issue during execution, please do due diligence to check whether the source of the error is in the user script/tool. If you suspect the source of the error comes from EnTK or tools below please open a ticket in the github repo and follow these steps:

### Enable verbose messages

Run your script again with ``RADICAL_VERBOSE=DEBUG`` and ``RADICAL_ENTK_VERBOSE=DEBUG``. Once these environment variables are set, a lot of messages will be displayed as they are written to the standard error stream. Please redirect these messages to a single file.

Example:
```
RADICAL_VERBOSE=DEBUG RADICAL_ENTK_VERBOSE=DEBUG python example.py &> verbose.log
```

Attach verbose.log to the ticket.

**NOTE**: If you suspect sections of this file to be pointing to the error you may consider mentioning that in the ticket via inline comments.

    Example:
    ```
    These lines of the log might be 
    talking about the error
    ```

### Client and remote logs in RP

When running a RP script or a tool that uses RP, multiple logs are created by the components of RP. A set of these logs are created in the current working directory on the client machine (where your script lies) and a set of logs are created on the remote machine (HPC) in a specific location. You can bring all the logs to the client by running the following cmd (on the client):

```
radical-pilot-fecth-logfiles <session id>
```

In order to determine the session id, you can look for a folder that is created on the client in current working directory. It should have the format ```re.session.*```. You can find the latest folder by doing ``ls -ltr`` (last is recent).

All the logfiles are brought to this re.session.* folder. Please zip this folder and attach to github ticket.

### Access to sandbox

Sometimes the above two steps may not be enough, the developers might require to take a look at the entire sandbox. You may either download the entire sandbox from the remote machine (using scp/gsiscp), zip the folder, and attach it to the ticket or provide access to the sandbox on the remote machine.
