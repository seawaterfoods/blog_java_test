import org.junit.jupiter.api.Test;

import java.util.HashSet;

public class leetCodeTest
{
    @Test
    public void leetCode02()
    {
        HashSet<String> diffLetter = new HashSet<>();
        String s = "dabvdf";
        char[] sChar = s.toCharArray();
        for (char str : sChar)
        {
            diffLetter.add(String.valueOf(str));
        }
        int ans = 0;
        HashSet<String> useLetter = new HashSet<>(diffLetter);
        int tmp = 0;
        for (char str : sChar)
        {
            if (useLetter.remove(String.valueOf(str)))
            {
                tmp++;
                System.out.println("str:" + str + "; tmp:" + tmp);
            } else
            {
                tmp = 0;
                useLetter = new HashSet<>(diffLetter);
            }
            if (useLetter.remove(String.valueOf(str)))
            {
                tmp++;
                System.out.println("str:" + str + "; tmp:" + tmp);
            }
            if (ans < tmp)
                ans = tmp;
        }
        System.out.println("ans :" + ans);

    }
}
