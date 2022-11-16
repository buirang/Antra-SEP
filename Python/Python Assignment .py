{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "5f4c570d",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "ename": "NameError",
     "evalue": "name 'Peolpe1' is not defined",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mNameError\u001b[0m                                 Traceback (most recent call last)",
      "Input \u001b[0;32mIn [7]\u001b[0m, in \u001b[0;36m<cell line: 6>\u001b[0;34m()\u001b[0m\n\u001b[1;32m      4\u001b[0m \u001b[38;5;66;03m# open people_1\u001b[39;00m\n\u001b[1;32m      5\u001b[0m People1 \u001b[38;5;241m=\u001b[39m pd\u001b[38;5;241m.\u001b[39mread_table(\u001b[38;5;124m'\u001b[39m\u001b[38;5;124mpeople_1.txt\u001b[39m\u001b[38;5;124m'\u001b[39m)\n\u001b[0;32m----> 6\u001b[0m \u001b[43mPeolpe1\u001b[49m\u001b[38;5;241m.\u001b[39mhead()\n",
      "\u001b[0;31mNameError\u001b[0m: name 'Peolpe1' is not defined"
     ]
    }
   ],
   "source": [
    "# Q1\n",
    "import pandas as pd\n",
    "\n",
    "# open people_1\n",
    "People1 = pd.read_table('people_1.txt')\n",
    "Peolpe1.head()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "ce39dd3b",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Q1\n",
    "import pandas as pd\n",
    "\n",
    "# open people_2\n",
    "People2 = pd.read_table('people_2.txt')\n",
    "People2.head()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "7a6f59ee-58d0-44df-bdee-bf8de1bb30e2",
   "metadata": {},
   "outputs": [],
   "source": [
    "#combine 2 files \n",
    "import pandas as pd\n",
    "#reading file 1 \n",
    "files = ['people_1.txt','people_2.txt']\n",
    "frames =[]\n",
    "\n",
    "for file in files:\n",
    "    frames.append(pd.read_csv(file))\n",
    "    \n",
    "df= pd.concat (frames, axis=1)\n",
    "df.to_csv ('people_3.txt', sep='\\t')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "894f3adf-db0b-457e-bea2-aa5dc5a9f223",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
