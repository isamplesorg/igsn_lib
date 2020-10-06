igsn_lib.time
=============

This libary provides some convenience methods for working with Julian dates.

A Julian Date is a floating point value that identifies the number of days before
or after ``-4713-11-24 12:00:00.000`` UTC. The range is unlimited, for example
the estimated age of the universe is about 13.787 billion years [#universe]_, and
that date is far outside of the common operating parameters for databases
and many programmatic representations of time.

In JD, the start of the universe can be reliably expressed as approximately:

.. jupyter-execute::

   import astropy.time
   tstart = astropy.time.Time(-13.787E9, format='jyear')
   jd = tstart.jd
   print(f"Universe started on Julian Date: {jd}")

The start of the universe can not be expressed in a python datetime:

 .. jupyter-execute::

    try:
      print(tstart.to_datetime(datetime.timezone.utc))
    except Exception as e:
      print(f"ERROR: with timezone: {e}")
    try:
      print(tstart.to_datetime())
    except Exception as e:
      print(f"ERROR: without timezone: {e}")

Julian dates are equally applicable to the current time (and future). For example,
this page was last edited:

.. jupyter-execute::

   import datetime
   jd = astropy.time.Time(datetime.datetime.now(datetime.timezone.utc)).jd
   print(f"It is now: {jd}")

Julian dates can be used to provide a consistent representation of geological time boundaries,
albeit at a higher resolution than the actual boundaries can be determined. For example the
Cenozoic [#cenozoic]_ period ranges from 66 million years ago to modern time. This can be
expressed in JD as:

.. jupyter-execute::

  cenozoic = (
    astropy.time.Time(-66E6, format='jyear').jd,
    astropy.time.Time(1950, format='jyear').jd,
  )
  print(f"The cenozoic period is JD {cenozoic[0]:.0f} to JD {cenozoic[1]:.0f}.")

Positions in archeologic and 'recent' geologic time are conventionally denoted
in years, measured backwards from the present, which is fixed to 1950 when the
precision requires it.

The start of the cenozoic is the Cretaceous-Paleogene Boundary (KBP, [#KBP]_)
determined to be 66.038 +- 0.025/0.049 Ma [#KBPage]_. That translates to JD:

.. jupyter-execute::

   c0 = astropy.time.Time(-66.038E6, format='jyear').jd
   c1 = 365*1E6*0.025
   c2 = 365*1E6*0.049
   print(f"Cenozoic start = {c0:.0f} +- {c1:.0f}/{c2:.0f}")


.. [#universe] https://en.wikipedia.org/wiki/Age_of_the_universe
.. [#cenozoic] https://en.wikipedia.org/wiki/Cenozoic
.. [#KBP] https://en.wikipedia.org/wiki/Cretaceous%E2%80%93Paleogene_extinction_event
.. [#KBPage] https://www.cugb.edu.cn/uploadCms/file/20600/20131028144132060.pdf



.. rubric:: Methods

The following methods are provided in the ``igsn_lib.time`` module:

.. autofuncsummary:: igsn_lib.time
   :functions:


.. rubric:: Detail

.. automodule:: igsn_lib.time
   :members:
   :undoc-members:
   :show-inheritance:
