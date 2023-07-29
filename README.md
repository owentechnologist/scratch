# scratch

Simple python code tests of various things I am learning/testing.

1. Create a virtual environment:

```
python3 -m venv venv 
```

2. Activate it:

```
source venv/bin/activate
```

On windows you would do:

```
venv\Scripts\activate
```

3. Add this requirements.txt to the project:

```
redis>=4.3.4
```

4. Run:

```
pip3 install -r requirements.txt
```

5. Execute the simplejsonsearch.py code (or another example) from the project directory:

```
python3 simplejsonsearch.py 
```

6. If you get errors it is most likely because you haven't configured the host and port and or username and password to your redis server

7. when you are done exploring this set of examples you can deactivate the virtual environment:

```
deactivate
```



