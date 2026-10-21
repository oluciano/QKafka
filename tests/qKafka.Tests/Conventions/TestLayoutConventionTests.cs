using System.Text.RegularExpressions;

namespace QKafka.Tests.Conventions;

/// <summary>
/// Enforces the test file layout rule (ai-method/workflows/test.md): one <c>FooUnitTests.cs</c> per production
/// <c>Foo.cs</c> at the mirrored path, <c>FooReliabilityTests.cs</c> in the reliability project, no orphans.
/// </summary>
public sealed class TestLayoutConventionTests
{
    private const string UnitSuffix = "UnitTests";
    private const string ReliabilitySuffix = "ReliabilityTests";
    private const string BaselineFile = "untested-baseline.txt";

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(2);

    private static readonly Regex LogicDeclaration = new(
        @"^\s*(?:(?:public|internal|private|protected|sealed|static|abstract|partial|readonly|unsafe)\s+)*(?:class|struct|record)\b",
        RegexOptions.Multiline | RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture,
        MatchTimeout);

    private static readonly Regex TestClassDeclaration = new(
        @"^\s*(?:(?:public|internal|sealed|static|abstract|partial)\s+)*class\s+(?<name>\w+)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture,
        MatchTimeout);

    private static readonly string Root = FindRepositoryRoot();

    private static string SourceDir => Path.Combine(Root, "src", "qKafka");

    private static string UnitDir => Path.Combine(Root, "tests", "qKafka.Tests");

    private static string ReliabilityDir => Path.Combine(Root, "tests", "qKafka.ReliabilityTests");

    [Fact]
    public void UnitTestFiles_MirrorAProductionFile()
    {
        var orphans = TestFiles(UnitDir, UnitSuffix)
            .Where(t => !File.Exists(Path.Combine(SourceDir, ToProductionPath(t, UnitDir, UnitSuffix))))
            .Select(t => Relative(t))
            .ToList();

        Assert.True(orphans.Count == 0, "Unit test files without a production file at the mirrored path:\n" + string.Join('\n', orphans));
    }

    [Fact]
    public void ReliabilityTestFiles_MirrorAProductionFile()
    {
        var orphans = TestFiles(ReliabilityDir, ReliabilitySuffix)
            .Where(t => !File.Exists(Path.Combine(SourceDir, ToProductionPath(t, ReliabilityDir, ReliabilitySuffix))))
            .Select(t => Relative(t))
            .ToList();

        Assert.True(orphans.Count == 0, "Reliability test files without a production file at the mirrored path:\n" + string.Join('\n', orphans));
    }

    [Fact]
    public void TestFiles_AreNamedWithTheCanonicalSuffix()
    {
        var offenders = AllTestSources(UnitDir)
            .Where(f => !IsExempt(f, UnitDir) && !Path.GetFileNameWithoutExtension(f).EndsWith(UnitSuffix, StringComparison.Ordinal))
            .Concat(AllTestSources(ReliabilityDir)
                .Where(f => !Path.GetFileNameWithoutExtension(f).EndsWith(ReliabilitySuffix, StringComparison.Ordinal)))
            .Select(f => Relative(f))
            .ToList();

        Assert.True(offenders.Count == 0, "Test files that do not end in UnitTests.cs / ReliabilityTests.cs:\n" + string.Join('\n', offenders));
    }

    [Fact]
    public void TestFiles_HoldOneClassNamedAfterTheFile()
    {
        var offenders = new List<string>();
        foreach (var file in TestFiles(UnitDir, UnitSuffix).Concat(TestFiles(ReliabilityDir, ReliabilitySuffix)))
        {
            var classes = TestClassDeclaration.Matches(File.ReadAllText(file)).Select(m => m.Groups["name"].Value).ToList();
            var expected = Path.GetFileNameWithoutExtension(file);
            if (classes.Count != 1 || !string.Equals(classes[0], expected, StringComparison.Ordinal))
            {
                offenders.Add($"{Relative(file)} declares [{string.Join(", ", classes)}], expected exactly [{expected}]");
            }
        }

        Assert.True(offenders.Count == 0, string.Join('\n', offenders));
    }

    [Fact]
    public void ProductionFilesWithLogic_HaveAUnitTestOrAreInTheBaseline()
    {
        var baseline = ReadBaseline();
        var missing = ProductionFilesWithLogic()
            .Where(p => !File.Exists(ToUnitTestPath(p)) && !baseline.Contains(p))
            .ToList();

        Assert.True(
            missing.Count == 0,
            "Production files with logic and no unit test (add <path>/<Name>UnitTests.cs):\n" + string.Join('\n', missing));
    }

    [Fact]
    public void Baseline_OnlyListsProductionFilesThatStillLackATest()
    {
        var logic = ProductionFilesWithLogic().ToHashSet(StringComparer.Ordinal);
        var stale = ReadBaseline()
            .Where(p => !logic.Contains(p) || File.Exists(ToUnitTestPath(p)))
            .ToList();

        Assert.True(
            stale.Count == 0,
            $"Remove from {BaselineFile} (file is gone, has no logic, or now has a unit test):\n" + string.Join('\n', stale));
    }

    private static string FindRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "qKafka.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("qKafka.sln not found above " + AppContext.BaseDirectory);
    }

    private static IEnumerable<string> AllTestSources(string dir) =>
        !Directory.Exists(dir)
            ? Enumerable.Empty<string>()
            : Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
                .Where(f => !IsBuildOutput(f) && !string.Equals(Path.GetFileName(f), "GlobalUsings.cs", StringComparison.Ordinal));

    private static IEnumerable<string> TestFiles(string dir, string suffix) =>
        AllTestSources(dir)
            .Where(f => !IsExempt(f, dir) && Path.GetFileNameWithoutExtension(f).EndsWith(suffix, StringComparison.Ordinal));

    private static IEnumerable<string> ProductionFilesWithLogic() =>
        Directory.EnumerateFiles(SourceDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(f) && LogicDeclaration.IsMatch(File.ReadAllText(f)))
            .Select(f => Path.GetRelativePath(SourceDir, f).Replace('\\', '/'))
            .OrderBy(p => p, StringComparer.Ordinal);

    private static bool IsBuildOutput(string path)
    {
        var parts = path.Replace('\\', '/').Split('/');
        return parts.Contains("obj") || parts.Contains("bin");
    }

    private static bool IsExempt(string file, string projectDir)
    {
        var relative = Path.GetRelativePath(projectDir, file).Replace('\\', '/');
        return relative.StartsWith("Conventions/", StringComparison.Ordinal) || relative.StartsWith("Support/", StringComparison.Ordinal);
    }

    private static string ToProductionPath(string testFile, string projectDir, string suffix)
    {
        var relative = Path.GetRelativePath(projectDir, testFile);
        return relative[..^(suffix.Length + ".cs".Length)] + ".cs";
    }

    private static string ToUnitTestPath(string productionRelativePath) =>
        Path.Combine(UnitDir, productionRelativePath[..^".cs".Length] + UnitSuffix + ".cs");

    private static string Relative(string path) => Path.GetRelativePath(Root, path).Replace('\\', '/');

    private static HashSet<string> ReadBaseline()
    {
        var path = Path.Combine(UnitDir, "Conventions", BaselineFile);
        return File.ReadAllLines(path)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith('#'))
            .ToHashSet(StringComparer.Ordinal);
    }
}
