package com.koi.mapreduce.invertedIndex;

public class test {

    public static void main(String[] args) {
        String a = "“We should start back,” Gared urged as the woods began to grow dark around them. “The wildlings are dead.”\n" +
                "\n" +
                "“Do the dead frighten you?” Ser Waymar Royce asked with just the hint of a smile.\n" +
                "\n" +
                "Gared did not rise to the bait. He was an old man, past fifty, and he had seen the lordlings come and go. “Dead is dead,” he said. “We have no business with the dead.”\n" +
                "\n" +
                "“Are they dead?” Royce asked softly. “What proof have we?”\n" +
                "\n" +
                "“Will saw them,” Gared said. “If he says they are dead, that’s proof enough for me.”\n" +
                "\n" +
                "Will had known they would drag him into the quarrel sooner or later. He wished it had been later rather than sooner. “My mother told me that dead men sing no songs,” he put in.\n" +
                "\n" +
                "“My wet nurse said the same thing, Will,” Royce replied. “Never believe anything you hear at a woman’s tit. There are things to be learned even from the dead.” His voice echoed, too loud in the twilit forest.\n" +
                "\n" +
                "“We have a long ride before us,” Gared pointed out. “Eight days, maybe nine. And night is falling.”\n" +
                "\n" +
                "Ser Waymar Royce glanced at the sky with disinterest. “It does that every day about this time. Are you unmanned by the dark, Gared?”";
        a = a.replaceAll("[^a-zA-Z0-9]", " ");
        String[] split = a.split("[ |\n]");

    }
}
